package com.karnataka.ksl.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Immutable;

import java.time.Instant;

/**
 * Audit Ledger Entry — Immutable, append-only.
 *
 * Every propagation event, conflict resolution, retry, and reconciliation step
 * is written here. This record is NEVER updated — only inserted. It answers:
 *
 *   - What changed?       (field_path, old_value, new_value)
 *   - Where did it come from? (source_system, source_event_id)
 *   - Where was it written?   (target_system, target_record_id)
 *   - Was there a conflict?   (conflict_detected, conflict_resolution_policy, conflict_winner)
 *   - Did it succeed?         (propagation_status)
 *   - Can it be rolled back?  (rollback_payload)
 */
@Entity
@Immutable
@Table(name = "audit_ledger", indexes = {
    @Index(name = "idx_audit_ubid", columnList = "ubid"),
    @Index(name = "idx_audit_correlation", columnList = "correlation_id"),
    @Index(name = "idx_audit_recorded_at", columnList = "recorded_at")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditLedgerEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    /** Groups all writes that originated from the same source event */
    @Column(name = "correlation_id", nullable = false, length = 50)
    private String correlationId;

    @Column(name = "ubid", nullable = false, length = 50)
    private String ubid;

    // ── Change Detail ─────────────────────────────────────────────────────────
    @Column(name = "field_path", length = 200)
    private String fieldPath;             // e.g. "registeredAddress.line1"

    @Column(name = "old_value", columnDefinition = "TEXT")
    private String oldValue;

    @Column(name = "new_value", columnDefinition = "TEXT")
    private String newValue;

    // ── Source ────────────────────────────────────────────────────────────────
    @Column(name = "source_system", nullable = false, length = 30)
    private String sourceSystem;

    @Column(name = "source_event_id", length = 100)
    private String sourceEventId;         // idempotency key from source

    @Column(name = "source_timestamp")
    private Instant sourceTimestamp;

    // ── Target ────────────────────────────────────────────────────────────────
    @Column(name = "target_system", nullable = false, length = 30)
    private String targetSystem;

    @Column(name = "target_record_id", length = 100)
    private String targetRecordId;

    // ── Propagation Result ───────────────────────────────────────────────────
    @Enumerated(EnumType.STRING)
    @Column(name = "propagation_status", nullable = false)
    private PropagationStatus propagationStatus;

    @Column(name = "retry_count")
    private int retryCount;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    private String failureReason;

    // ── Conflict Resolution ──────────────────────────────────────────────────
    @Column(name = "conflict_detected")
    private boolean conflictDetected;

    @Column(name = "conflict_resolution_policy", length = 50)
    private String conflictResolutionPolicy;  // SOURCE_HIERARCHY | LATEST_TIMESTAMP | MANUAL

    @Column(name = "conflict_winner", length = 30)
    private String conflictWinner;

    @Column(name = "conflict_explanation", columnDefinition = "TEXT")
    private String conflictExplanation;

    // ── Rollback Support ─────────────────────────────────────────────────────
    @Column(name = "rollback_payload", columnDefinition = "TEXT")
    private String rollbackPayload;       // JSON snapshot of pre-change state

    @CreationTimestamp
    @Column(name = "recorded_at", nullable = false, updatable = false)
    private Instant recordedAt;

    public enum PropagationStatus {
        SUCCESS,
        PENDING,
        RETRYING,
        FAILED,
        CONFLICT_HELD,   // held for manual resolution
        ROLLED_BACK
    }
}
