package com.karnataka.ksl.audit;

import com.karnataka.ksl.conflict.ConflictResolutionEngine;
import com.karnataka.ksl.model.AuditLedgerEntry;
import com.karnataka.ksl.repository.AuditLedgerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Audit Service
 *
 * The only way anything gets into the audit ledger. All writes are in
 * REQUIRES_NEW propagation so audit entries are committed even if the
 * surrounding transaction rolls back — we never lose audit history.
 *
 * The audit trail answers every question the Karnataka Commerce & Industries
 * team needs:
 *   - What changed?
 *   - When did it change?
 *   - Where did it come from?
 *   - Where was it written?
 *   - Was there a conflict? How was it resolved? Why?
 *   - Can we roll back?
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AuditService {

    private final AuditLedgerRepository auditLedgerRepository;

    /**
     * Log a successful or failed propagation.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public AuditLedgerEntry logPropagation(
            String correlationId,
            String ubid,
            String sourceSystem,
            String targetSystem,
            String targetRecordId,
            AuditLedgerEntry.PropagationStatus status,
            String failureReason,
            String rollbackPayload) {

        AuditLedgerEntry entry = AuditLedgerEntry.builder()
            .correlationId(correlationId)
            .ubid(ubid)
            .sourceSystem(sourceSystem)
            .targetSystem(targetSystem)
            .targetRecordId(targetRecordId)
            .propagationStatus(status)
            .failureReason(failureReason)
            .rollbackPayload(rollbackPayload)
            .conflictDetected(false)
            .retryCount(0)
            .build();

        AuditLedgerEntry saved = auditLedgerRepository.save(entry);
        log.debug("[Audit] Logged propagation: id={} correlation={} {} → {} status={}",
                  saved.getId(), correlationId, sourceSystem, targetSystem, status);
        return saved;
    }

    /**
     * Log a conflict detection and its resolution.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public AuditLedgerEntry logConflict(
            String correlationId,
            String ubid,
            ConflictResolutionEngine.ResolutionResult resolution) {

        String winnerSystem = resolution.winner() != null
            ? resolution.winner().sourceSystem() : "NONE";
        String loserSystem = resolution.loser() != null
            ? resolution.loser().sourceSystem() : "NONE";

        AuditLedgerEntry entry = AuditLedgerEntry.builder()
            .correlationId(correlationId)
            .ubid(ubid)
            .sourceSystem(loserSystem)
            .targetSystem(winnerSystem)
            .propagationStatus(
                resolution.requiresManualReview()
                    ? AuditLedgerEntry.PropagationStatus.CONFLICT_HELD
                    : AuditLedgerEntry.PropagationStatus.SUCCESS)
            .conflictDetected(true)
            .conflictResolutionPolicy(resolution.policy().name())
            .conflictWinner(winnerSystem)
            .conflictExplanation(resolution.explanation())
            .retryCount(0)
            .build();

        AuditLedgerEntry saved = auditLedgerRepository.save(entry);
        log.info("[Audit] Conflict logged: id={} ubid={} policy={} winner={}",
                 saved.getId(), ubid, resolution.policy(), winnerSystem);
        return saved;
    }

    /**
     * Log a retry attempt.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public AuditLedgerEntry logRetry(
            String correlationId, String ubid,
            String sourceSystem, String targetSystem,
            int retryCount, String reason) {

        AuditLedgerEntry entry = AuditLedgerEntry.builder()
            .correlationId(correlationId)
            .ubid(ubid)
            .sourceSystem(sourceSystem)
            .targetSystem(targetSystem)
            .propagationStatus(AuditLedgerEntry.PropagationStatus.RETRYING)
            .retryCount(retryCount)
            .failureReason(reason)
            .conflictDetected(false)
            .build();

        return auditLedgerRepository.save(entry);
    }
}
