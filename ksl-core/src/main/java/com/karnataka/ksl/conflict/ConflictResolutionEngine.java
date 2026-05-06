package com.karnataka.ksl.conflict;

import com.karnataka.ksl.model.UniversalBusinessRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Conflict Resolution Engine
 *
 * When two updates for the same UBID arrive within a conflict window, this engine:
 *   1. Detects the conflict
 *   2. Applies a configured resolution policy
 *   3. Produces a human-readable explanation (important for audit + reversibility)
 *
 * Resolution Policies (configurable per field):
 *
 *   SOURCE_HIERARCHY — Predefined authority per field:
 *     - registeredAddress.*  → SWS is authoritative (business registers address there)
 *     - authorisedSignatory.* → Department is authoritative (they hold the legal record)
 *     - licenseNumbers.*     → Department is authoritative
 *     - primaryPhone/Email   → SWS is authoritative
 *     - operationalStatus    → Department is authoritative
 *
 *   LATEST_TIMESTAMP — The more recent update wins.
 *     Used when field-level authority is ambiguous.
 *
 *   MANUAL_HOLD — Conflict flagged for human review.
 *     Used for high-risk fields (PAN, CIN) where silent resolution is not safe.
 */
@Component
@Slf4j
public class ConflictResolutionEngine {

    @Value("${ksl.conflict.window-seconds:30}")
    private long conflictWindowSeconds;

    // Field-level authority mapping: field prefix → authoritative source
    private static final Map<String, String> FIELD_AUTHORITY = Map.of(
        "registeredAddress",    "SWS",
        "primaryPhone",         "SWS",
        "primaryEmail",         "SWS",
        "businessName",         "SWS",
        "authorisedSignatory",  "DEPARTMENT",
        "operationalStatus",    "DEPARTMENT",
        "licenseNumbers",       "DEPARTMENT"
    );

    // Fields that must NEVER be auto-resolved — always flagged for manual review
    private static final List<String> MANUAL_HOLD_FIELDS = List.of("panNumber", "cinNumber");

    /**
     * Determine if two updates constitute a conflict.
     * A conflict occurs when two updates for the same UBID touch the same field
     * within the configured conflict window.
     */
    public boolean isConflict(PendingUpdate incoming, PendingUpdate existing) {
        if (!incoming.ubid().equals(existing.ubid())) return false;

        // Check if they're within the conflict window
        Duration gap = Duration.between(existing.arrivedAt(), incoming.arrivedAt()).abs();
        if (gap.toSeconds() > conflictWindowSeconds) return false;

        // Check if they touch overlapping fields
        return touchesOverlappingFields(incoming, existing);
    }

    /**
     * Resolve the conflict and return a ResolutionResult with full explanation.
     */
    public ResolutionResult resolve(PendingUpdate update1, PendingUpdate update2) {
        log.info("[Conflict] Resolving conflict for UBID={} between source={} and source={}",
                 update1.ubid(), update1.sourceSystem(), update2.sourceSystem());

        // Check for manual-hold fields first
        if (touchesManualHoldFields(update1) || touchesManualHoldFields(update2)) {
            return ResolutionResult.manualHold(update1, update2,
                "Field requires human review (PAN/CIN changes cannot be auto-resolved)");
        }

        // Identify the field and look up its authority
        String field = findConflictingField(update1, update2);
        String authority = FIELD_AUTHORITY.getOrDefault(getFieldPrefix(field), null);

        if (authority != null) {
            return applySourceHierarchy(update1, update2, field, authority);
        }

        // Fall back to latest timestamp
        return applyLatestTimestamp(update1, update2, field);
    }

    private ResolutionResult applySourceHierarchy(
            PendingUpdate u1, PendingUpdate u2, String field, String authority) {

        boolean u1IsAuthority = authority.equals("SWS")
            ? u1.sourceSystem().equals("SWS")
            : !u1.sourceSystem().equals("SWS");

        PendingUpdate winner = u1IsAuthority ? u1 : u2;
        PendingUpdate loser  = u1IsAuthority ? u2 : u1;

        String explanation = String.format(
            "Field '%s' is under %s authority per SOURCE_HIERARCHY policy. " +
            "Update from %s accepted; update from %s discarded. " +
            "Conflict window: %ds.",
            field, authority, winner.sourceSystem(), loser.sourceSystem(), conflictWindowSeconds);

        log.info("[Conflict] SOURCE_HIERARCHY resolution: winner={}, explanation={}",
                 winner.sourceSystem(), explanation);

        return new ResolutionResult(
            winner, loser, ResolutionPolicy.SOURCE_HIERARCHY, explanation, false);
    }

    private ResolutionResult applyLatestTimestamp(
            PendingUpdate u1, PendingUpdate u2, String field) {

        PendingUpdate winner = u1.arrivedAt().isAfter(u2.arrivedAt()) ? u1 : u2;
        PendingUpdate loser  = winner == u1 ? u2 : u1;

        String explanation = String.format(
            "No explicit authority defined for field '%s'. " +
            "LATEST_TIMESTAMP policy applied: %s (%s) beats %s (%s).",
            field, winner.sourceSystem(), winner.arrivedAt(),
            loser.sourceSystem(), loser.arrivedAt());

        return new ResolutionResult(
            winner, loser, ResolutionPolicy.LATEST_TIMESTAMP, explanation, false);
    }

    private boolean touchesOverlappingFields(PendingUpdate a, PendingUpdate b) {
        // In a real implementation, compare the changed fields in each UBS record
        // Simplified: compare if the same UBID is touched on the same field group
        return a.changedFieldPath() != null && a.changedFieldPath().equals(b.changedFieldPath());
    }

    private boolean touchesManualHoldFields(PendingUpdate update) {
        return update.changedFieldPath() != null &&
               MANUAL_HOLD_FIELDS.stream().anyMatch(f -> update.changedFieldPath().startsWith(f));
    }

    private String findConflictingField(PendingUpdate u1, PendingUpdate u2) {
        return u1.changedFieldPath() != null ? u1.changedFieldPath() : "unknown";
    }

    private String getFieldPrefix(String fieldPath) {
        if (fieldPath == null) return "";
        int dot = fieldPath.indexOf('.');
        return dot > 0 ? fieldPath.substring(0, dot) : fieldPath;
    }

    // ── Value Objects ──────────────────────────────────────────────────────────

    public record PendingUpdate(
        String ubid,
        String sourceSystem,
        String sourceEventId,
        UniversalBusinessRecord record,
        String changedFieldPath,
        Instant arrivedAt
    ) {}

    public record ResolutionResult(
        PendingUpdate winner,
        PendingUpdate loser,
        ResolutionPolicy policy,
        String explanation,   // Human-readable, stored in audit trail
        boolean requiresManualReview
    ) {
        static ResolutionResult manualHold(PendingUpdate u1, PendingUpdate u2, String reason) {
            return new ResolutionResult(null, null, ResolutionPolicy.MANUAL_HOLD, reason, true);
        }
    }

    public enum ResolutionPolicy {
        SOURCE_HIERARCHY,
        LATEST_TIMESTAMP,
        MANUAL_HOLD
    }
}
