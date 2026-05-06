package com.karnataka.ksl.adapter;

import com.karnataka.ksl.model.UniversalBusinessRecord;

import java.util.List;
import java.util.Optional;

/**
 * DepartmentAdapter — The core abstraction of the Adapter Pattern.
 *
 * Each legacy department system gets exactly one implementation of this interface.
 * The adapter knows:
 *   (a) How to read from that system (via its API, DB view, SOAP, etc.)
 *   (b) How to write back to that system
 *   (c) How to translate between UBS ↔ the department's native schema
 *
 * KSL core NEVER calls department APIs directly — it always goes through an adapter.
 * This means adding a new department = writing one new adapter class. No other
 * changes needed anywhere in KSL.
 */
public interface DepartmentAdapter {

    /**
     * Unique code for this department. Must match shadow_registry.department_code.
     * Examples: "SWS", "SHOPS_EST", "FACTORIES", "POLLUTION_CTRL", "FIRE_SAFETY"
     */
    String getDepartmentCode();

    /**
     * Human-readable name for UI and audit trail.
     */
    String getDepartmentName();

    /**
     * Describes how this adapter can discover changes.
     */
    ChangeDiscoveryCapability getChangeDiscoveryCapability();

    /**
     * Fetch the current record for a given legacy ID and translate it to UBS.
     * Returns empty if the record doesn't exist in this department.
     */
    Optional<UniversalBusinessRecord> fetchByLegacyId(String legacyId);

    /**
     * Fetch by UBID — the adapter resolves UBID → legacyId via the shadow registry
     * before calling the underlying system.
     */
    Optional<UniversalBusinessRecord> fetchByUbid(String ubid);

    /**
     * Write a UBS record to this department system.
     * The adapter translates UBS → department schema before writing.
     *
     * @param ubsRecord   The canonical record to write
     * @param idempotencyKey Unique key; if the adapter has already processed this
     *                    key, it returns the previous result without re-writing.
     * @return WriteResult containing success status and the target record's ID
     */
    WriteResult write(UniversalBusinessRecord ubsRecord, String idempotencyKey);

    /**
     * Poll for changes since the last known hash. Used by Ghost Poller for
     * systems that don't emit webhooks or events.
     *
     * @param legacyId The department's own record identifier
     * @param lastKnownHash SHA-256 of the last known serialised record
     * @return Empty if nothing changed; present if the record differs from the hash
     */
    Optional<UniversalBusinessRecord> pollForChanges(String legacyId, String lastKnownHash);

    /**
     * Fetch all records changed since a given timestamp.
     * Only applicable when capability includes SNAPSHOT_DIFF or API_EVENTS.
     */
    List<UniversalBusinessRecord> fetchChangedSince(java.time.Instant since);

    /**
     * Health check — returns true if the underlying system is reachable.
     * Used by circuit breaker logic to avoid hammering a down system.
     */
    boolean isHealthy();

    enum ChangeDiscoveryCapability {
        WEBHOOK,        // System pushes events to KSL
        API_EVENTS,     // System exposes a "changes since" API
        SNAPSHOT_DIFF,  // KSL fetches full snapshot and diffs
        ROW_HASH        // KSL polls individual rows and compares hash
    }

    record WriteResult(
        boolean success,
        String targetRecordId,
        String idempotencyKey,
        String errorMessage,
        boolean wasAlreadyProcessed  // true = idempotent replay, not a new write
    ) {
        public static WriteResult success(String targetRecordId, String idempotencyKey) {
            return new WriteResult(true, targetRecordId, idempotencyKey, null, false);
        }
        public static WriteResult alreadyProcessed(String targetRecordId, String idempotencyKey) {
            return new WriteResult(true, targetRecordId, idempotencyKey, null, true);
        }
        public static WriteResult failure(String errorMessage) {
            return new WriteResult(false, null, null, errorMessage, false);
        }
    }
}
