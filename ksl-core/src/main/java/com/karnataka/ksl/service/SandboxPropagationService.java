package com.karnataka.ksl.service;

import com.karnataka.ksl.adapter.DepartmentAdapter;
import com.karnataka.ksl.audit.AuditService;
import com.karnataka.ksl.conflict.ConflictResolutionEngine;
import com.karnataka.ksl.model.AuditLedgerEntry;
import com.karnataka.ksl.model.UniversalBusinessRecord;
import com.karnataka.ksl.repository.ShadowRegistryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SandboxPropagationService
 *
 * Active ONLY when Kafka is NOT configured (sandbox / demo mode).
 * Provides the same propagation guarantees as the Kafka-backed pipeline
 * but executes synchronously in-process:
 *
 *   SWS → fan-out to every department in shadow registry
 *   DEPT → write through to SWS
 *   Conflict detection and audit ledger writes work identically to production.
 *
 * This means /propagate/sws and /webhook/{dept} return a correlationId that
 * can be looked up via /audit/correlation/{id} and will return real entries.
 */
@Service
@ConditionalOnMissingBean(KafkaTemplate.class)
@Slf4j
@RequiredArgsConstructor
public class SandboxPropagationService {

    private final Map<String, DepartmentAdapter> adapters;
    private final ShadowRegistryRepository shadowRegistry;
    private final AuditService auditService;
    private final ConflictResolutionEngine conflictEngine;

    // In-memory idempotency store (sandbox: no Redis/DB persistence needed)
    private final ConcurrentHashMap<String, Boolean> processedEvents = new ConcurrentHashMap<>();
    // Conflict detection: last in-flight update per UBID
    private final ConcurrentHashMap<String, ConflictResolutionEngine.PendingUpdate> inFlightByUbid
        = new ConcurrentHashMap<>();

    /**
     * Process a SWS-originated change: fan out to all matching departments.
     * Returns the correlationId for tracing.
     */
    public String processSWSEvent(UniversalBusinessRecord record) {
        String correlationId = UUID.randomUUID().toString();
        String eventId       = UUID.randomUUID().toString();

        if (processedEvents.putIfAbsent(eventId, true) != null) {
            log.info("[Sandbox] Duplicate SWS event skipped: {}", eventId);
            return correlationId;
        }

        log.info("[Sandbox] Processing SWS→DEPT propagation: ubid={} correlationId={}",
                 record.getUbid(), correlationId);

        List<String> targetDepts = shadowRegistry.findByUbid(record.getUbid()).stream()
            .map(e -> e.getDepartmentCode())
            .filter(code -> !code.equals("SWS"))
            .toList();

        if (targetDepts.isEmpty()) {
            log.warn("[Sandbox] No shadow registry entries found for UBID={}", record.getUbid());
            // Still log an audit entry so the correlation ID returns something useful
            auditService.logPropagation(correlationId, record.getUbid(),
                "SWS", "NONE",
                null,
                AuditLedgerEntry.PropagationStatus.FAILED,
                "No shadow registry entries found for UBID " + record.getUbid(),
                null);
            return correlationId;
        }

        for (String deptCode : targetDepts) {
            DepartmentAdapter adapter = adapters.get(deptCode);
            if (adapter == null) {
                log.warn("[Sandbox] No adapter registered for dept={}", deptCode);
                auditService.logPropagation(correlationId, record.getUbid(),
                    "SWS", deptCode, null,
                    AuditLedgerEntry.PropagationStatus.FAILED,
                    "No adapter found for department: " + deptCode, null);
                continue;
            }

            String idempotencyKey = correlationId + ":" + deptCode;
            DepartmentAdapter.WriteResult result = adapter.write(record, idempotencyKey);

            auditService.logPropagation(
                correlationId, record.getUbid(),
                "SWS", deptCode,
                result.targetRecordId(),
                result.success()
                    ? AuditLedgerEntry.PropagationStatus.SUCCESS
                    : AuditLedgerEntry.PropagationStatus.FAILED,
                result.errorMessage(),
                null
            );

            log.info("[Sandbox] SWS→{}: ubid={} success={} target={}",
                     deptCode, record.getUbid(), result.success(), result.targetRecordId());
        }

        return correlationId;
    }

    /**
     * Process a department-originated change: conflict check then write to SWS.
     * Returns the correlationId for tracing.
     */
    public String processDeptEvent(UniversalBusinessRecord record, String deptCode) {
        String correlationId = UUID.randomUUID().toString();
        String eventId       = UUID.randomUUID().toString();

        if (processedEvents.putIfAbsent(eventId, true) != null) {
            log.info("[Sandbox] Duplicate dept event skipped: {}", eventId);
            return correlationId;
        }

        log.info("[Sandbox] Processing DEPT→SWS propagation: ubid={} dept={} correlationId={}",
                 record.getUbid(), deptCode, correlationId);

        // ── Conflict Detection ───────────────────────────────────────────────
        ConflictResolutionEngine.PendingUpdate incoming = new ConflictResolutionEngine.PendingUpdate(
            record.getUbid(), deptCode, eventId, record, null, Instant.now());

        ConflictResolutionEngine.PendingUpdate existing = inFlightByUbid.get(record.getUbid());
        boolean conflictDetected = false;

        if (existing != null && conflictEngine.isConflict(incoming, existing)) {
            ConflictResolutionEngine.ResolutionResult resolution =
                conflictEngine.resolve(incoming, existing);
            auditService.logConflict(correlationId, record.getUbid(), resolution);
            conflictDetected = true;
            log.info("[Sandbox] Conflict detected for UBID={}: policy={} winner={}",
                     record.getUbid(), resolution.policy(), resolution.winner());

            if (resolution.requiresManualReview()) {
                log.warn("[Sandbox] UBID={} held for manual review", record.getUbid());
                return correlationId; // Don't propagate — held for manual review
            }
        } else {
            inFlightByUbid.put(record.getUbid(), incoming);
        }

        // ── Write to SWS ─────────────────────────────────────────────────────
        DepartmentAdapter swsAdapter = adapters.get("SWS");
        if (swsAdapter == null) {
            log.warn("[Sandbox] No SWS adapter registered — logging FAILED audit entry");
            auditService.logPropagation(correlationId, record.getUbid(),
                deptCode, "SWS", null,
                AuditLedgerEntry.PropagationStatus.FAILED,
                "No SWS adapter registered (sandbox — adapter map: " + adapters.keySet() + ")",
                null);
            return correlationId;
        }

        String idempotencyKey = correlationId + ":SWS";
        DepartmentAdapter.WriteResult result = swsAdapter.write(record, idempotencyKey);

        auditService.logPropagation(
            correlationId, record.getUbid(),
            deptCode, "SWS",
            result.targetRecordId(),
            result.success()
                ? AuditLedgerEntry.PropagationStatus.SUCCESS
                : AuditLedgerEntry.PropagationStatus.FAILED,
            result.errorMessage(),
            null
        );

        log.info("[Sandbox] {}→SWS: ubid={} success={} conflict={}",
                 deptCode, record.getUbid(), result.success(), conflictDetected);

        return correlationId;
    }
}