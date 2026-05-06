package com.karnataka.ksl.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;

/**
 * Shadow Registry Entry
 *
 * The master map that tells KSL: "for UBID X, the corresponding record
 * in the Factories system is record ID Y with field-name 'factory_reg_no'."
 *
 * This is the heart of the non-invasive approach — we never touch the
 * department systems' schemas, but we maintain a durable mapping here.
 */
@Entity
@Table(
    name = "shadow_registry",
    uniqueConstraints = @UniqueConstraint(columnNames = {"ubid", "department_code"})
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShadowRegistryEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    /** Karnataka UBID — the universal join key */
    @Column(name = "ubid", nullable = false, length = 50)
    private String ubid;

    /** Short code for the department: SWS, SHOPS_EST, FACTORIES, POLLUTION_CTRL, etc. */
    @Column(name = "department_code", nullable = false, length = 30)
    private String departmentCode;

    /** The department's own primary key for this business */
    @Column(name = "legacy_id", nullable = false, length = 100)
    private String legacyId;

    /** Human-readable field name for the legacy ID (e.g. "registration_no", "factory_id") */
    @Column(name = "legacy_id_field_name", length = 50)
    private String legacyIdFieldName;

    /** Endpoint or table where this record lives */
    @Column(name = "system_endpoint", length = 500)
    private String systemEndpoint;

    /** Current sync state for this business in this department */
    @Enumerated(EnumType.STRING)
    @Column(name = "sync_status", nullable = false)
    private SyncStatus syncStatus;

    /** Hash of last known record — used by Ghost Poller to detect changes */
    @Column(name = "last_known_hash", length = 64)
    private String lastKnownHash;

    @Column(name = "last_synced_at")
    private Instant lastSyncedAt;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private Instant updatedAt;

    public enum SyncStatus {
        ACTIVE,        // Healthy, syncing normally
        PENDING_SYNC,  // Needs to be synced
        CONFLICT,      // Conflict detected, awaiting resolution
        STALE,         // Hash check shows drift; polling will reconcile
        INACTIVE       // Department removed this record
    }
}
