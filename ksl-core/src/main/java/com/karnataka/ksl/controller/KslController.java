package com.karnataka.ksl.controller;

import com.karnataka.ksl.kafka.KslEventProducer;
import com.karnataka.ksl.model.AuditLedgerEntry;
import com.karnataka.ksl.model.ShadowRegistryEntry;
import com.karnataka.ksl.model.UniversalBusinessRecord;
import com.karnataka.ksl.repository.AuditLedgerRepository;
import com.karnataka.ksl.repository.ShadowRegistryRepository;
import com.karnataka.ksl.service.SandboxPropagationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * KSL REST API — Dashboard and Management Endpoints
 *
 * Used by the React UI to display:
 *   - Shadow registry state
 *   - Audit trail per UBID
 *   - Active conflicts needing review
 *   - System health
 *
 * Also exposes webhook ingestion endpoints for departments that support webhooks.
 */
@RestController
@RequestMapping("/api/v1/ksl")
@Tag(name = "K-State Ledger API", description = "KSL Management and Monitoring")
@Slf4j
public class KslController {

    private final ShadowRegistryRepository shadowRegistry;
    private final AuditLedgerRepository auditLedger;

    @Nullable
    private final KslEventProducer eventProducer;

    @Nullable
    private final SandboxPropagationService sandboxPropagationService;

    @Autowired
    public KslController(ShadowRegistryRepository shadowRegistry,
                         AuditLedgerRepository auditLedger,
                         @Nullable KslEventProducer eventProducer,
                         @Nullable SandboxPropagationService sandboxPropagationService) {
        this.shadowRegistry = shadowRegistry;
        this.auditLedger = auditLedger;
        this.eventProducer = eventProducer;
        this.sandboxPropagationService = sandboxPropagationService;
    }

    // ── Shadow Registry ───────────────────────────────────────────────────────

    @GetMapping("/registry/{ubid}")
    @Operation(summary = "Get all shadow registry entries for a UBID")
    public ResponseEntity<List<ShadowRegistryEntry>> getRegistryByUbid(
            @PathVariable String ubid) {
        return ResponseEntity.ok(shadowRegistry.findByUbid(ubid));
    }

    @GetMapping("/registry")
    @Operation(summary = "List all shadow registry entries (paginated)")
    public ResponseEntity<Page<ShadowRegistryEntry>> listRegistry(Pageable pageable) {
        return ResponseEntity.ok(shadowRegistry.findAll(pageable));
    }

    @PostMapping("/registry")
    @Operation(summary = "Register a new UBID → LegacyID mapping")
    public ResponseEntity<ShadowRegistryEntry> createRegistryEntry(
            @Valid @RequestBody ShadowRegistryEntry entry) {
        entry.setSyncStatus(ShadowRegistryEntry.SyncStatus.ACTIVE);
        return ResponseEntity.ok(shadowRegistry.save(entry));
    }

    // ── Audit Trail ───────────────────────────────────────────────────────────

    @GetMapping("/audit/{ubid}")
    @Operation(summary = "Get full audit trail for a UBID")
    public ResponseEntity<List<AuditLedgerEntry>> getAuditTrail(@PathVariable String ubid) {
        return ResponseEntity.ok(auditLedger.findByUbidOrderByRecordedAtDesc(ubid));
    }

    @GetMapping("/audit/correlation/{correlationId}")
    @Operation(summary = "Trace a specific propagation by correlation ID")
    public ResponseEntity<List<AuditLedgerEntry>> getByCorrelation(
            @PathVariable String correlationId) {
        return ResponseEntity.ok(auditLedger.findByCorrelationIdOrderByRecordedAtAsc(correlationId));
    }

    @GetMapping("/audit/conflicts")
    @Operation(summary = "List all unresolved conflicts")
    public ResponseEntity<List<AuditLedgerEntry>> getConflicts() {
        return ResponseEntity.ok(
            auditLedger.findByPropagationStatus(AuditLedgerEntry.PropagationStatus.CONFLICT_HELD));
    }

    @GetMapping("/audit/all")
    @Operation(summary = "Get all audit entries (paginated), newest first")
    public ResponseEntity<Page<AuditLedgerEntry>> getAllAuditEntries(Pageable pageable) {
        return ResponseEntity.ok(auditLedger.findAllByOrderByRecordedAtDesc(pageable));
    }

    @PostMapping("/simulate/conflict")
    @Operation(summary = "Simulate a conflict: fire two updates for the same UBID from different sources")
    public ResponseEntity<Map<String, Object>> simulateConflict(
            @RequestBody UniversalBusinessRecord record) {

        if (sandboxPropagationService == null) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "Conflict simulation only available in sandbox mode (no Kafka)"));
        }

        log.info("[Simulate] Triggering conflict simulation for UBID={}", record.getUbid());

        // Fire SWS event
        record.setSourceSystem("SWS");
        String corrId1 = sandboxPropagationService.processSWSEvent(record);

        // Immediately fire a department event for same UBID (triggers conflict window)
        record.setSourceSystem("SHOPS_EST");
        String corrId2 = sandboxPropagationService.processDeptEvent(record, "SHOPS_EST");

        return ResponseEntity.accepted().body(Map.of(
            "swsCorrelationId", corrId1,
            "deptCorrelationId", corrId2,
            "message", "Two updates fired for UBID=" + record.getUbid() +
                       ". Check /audit/conflicts and /audit/" + record.getUbid() + " for results."
        ));
    }

    // ── Webhook Ingestion ────────────────────────────────────────────────────

    @PostMapping("/webhook/{deptCode}")
    @Operation(summary = "Ingest a webhook event from a department system")
    public ResponseEntity<Map<String, String>> ingestWebhook(
            @PathVariable String deptCode,
            @RequestBody UniversalBusinessRecord record) {

        log.info("[Webhook] Received from dept={} ubid={}", deptCode, record.getUbid());
        record.setSourceSystem(deptCode);

        String correlationId;
        if (sandboxPropagationService != null) {
            correlationId = sandboxPropagationService.processDeptEvent(record, deptCode);
        } else {
            correlationId = eventProducer.publishDepartmentChangeEvent(record, deptCode, "WEBHOOK");
        }
        return ResponseEntity.accepted().body(Map.of("correlationId", correlationId));
    }

    // ── Manual SWS Event Trigger (for testing) ────────────────────────────────

    @PostMapping("/propagate/sws")
    @Operation(summary = "Manually trigger a SWS → Departments propagation (test/sandbox)")
    public ResponseEntity<Map<String, String>> triggerSwsPropagation(
            @RequestBody UniversalBusinessRecord record) {

        log.info("[API] Manual SWS propagation trigger for UBID={}", record.getUbid());
        record.setSourceSystem("SWS");

        String correlationId;
        if (sandboxPropagationService != null) {
            correlationId = sandboxPropagationService.processSWSEvent(record);
        } else {
            correlationId = eventProducer.publishSwsChangeEvent(record);
        }
        return ResponseEntity.accepted().body(Map.of("correlationId", correlationId));
    }

    // ── Dashboard Stats ───────────────────────────────────────────────────────

    @GetMapping("/stats")
    @Operation(summary = "Get KSL dashboard statistics")
    public ResponseEntity<KslStats> getStats() {
        long totalRegistrations = shadowRegistry.count();
        long activeRegistrations = shadowRegistry.countBySyncStatus(ShadowRegistryEntry.SyncStatus.ACTIVE);
        long conflicts = auditLedger.countByPropagationStatus(AuditLedgerEntry.PropagationStatus.CONFLICT_HELD);
        long failedPropagations = auditLedger.countByPropagationStatus(AuditLedgerEntry.PropagationStatus.FAILED);
        long successfulPropagations = auditLedger.countByPropagationStatus(AuditLedgerEntry.PropagationStatus.SUCCESS);

        return ResponseEntity.ok(new KslStats(
            totalRegistrations, activeRegistrations,
            conflicts, failedPropagations, successfulPropagations));
    }

    public record KslStats(
        long totalRegistrations,
        long activeRegistrations,
        long unresolvedConflicts,
        long failedPropagations,
        long successfulPropagations
    ) {}
}