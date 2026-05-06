package com.karnataka.ksl.polling;

import com.karnataka.ksl.adapter.DepartmentAdapter;
import com.karnataka.ksl.kafka.KslEventProducer;
import com.karnataka.ksl.model.ShadowRegistryEntry;
import com.karnataka.ksl.model.UniversalBusinessRecord;
import com.karnataka.ksl.repository.ShadowRegistryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.lang.Nullable;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Ghost Poller — Adaptive Change Detection
 *
 * For department systems that do NOT emit webhooks or events, KSL must actively
 * discover changes. The Ghost Poller uses "adaptive hashing": it computes a
 * SHA-256 fingerprint of each record and compares it to the last known hash.
 * Any difference triggers a change event, which flows into the Kafka pipeline
 * exactly as if the department had emitted it directly.
 *
 * Adaptive behaviour:
 *   - Records that change frequently get polled more aggressively.
 *   - Stable records fall back to a slow poll rate (saves DB load).
 *   - If a department system is unhealthy (circuit breaker open), polling pauses.
 *
 * Poll rates (configurable per department):
 *   - High-activity records: every 2 minutes
 *   - Normal records: every 15 minutes
 *   - Slow records (unchanged for 7 days): every 2 hours
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class GhostPoller {

    private final ShadowRegistryRepository shadowRegistry;
    private final Map<String, DepartmentAdapter> adapters; // injected as a map by Spring
    @Nullable
    private final KslEventProducer eventProducer;

    // Track change frequency per legacy ID for adaptive rate control
    private final ConcurrentHashMap<String, Integer> changeCounters = new ConcurrentHashMap<>();

    /**
     * High-frequency poll — runs every 2 minutes.
     * Targets: records with recent change activity.
     */
    @Scheduled(fixedDelayString = "${ksl.polling.high-frequency-ms:120000}")
    public void pollHighActivity() {
        log.debug("[GhostPoller] HIGH-FREQ poll starting");
        pollByPriority(PollPriority.HIGH);
    }

    /**
     * Normal poll — runs every 15 minutes.
     * Targets: all ROW_HASH-capable departments not in high-activity mode.
     */
    @Scheduled(fixedDelayString = "${ksl.polling.normal-frequency-ms:900000}")
    public void pollNormal() {
        log.debug("[GhostPoller] NORMAL poll starting");
        pollByPriority(PollPriority.NORMAL);
    }

    /**
     * Slow poll — runs every 2 hours.
     * Targets: records stable for more than 7 days.
     */
    @Scheduled(fixedDelayString = "${ksl.polling.slow-frequency-ms:7200000}")
    public void pollSlow() {
        log.debug("[GhostPoller] SLOW poll starting");
        pollByPriority(PollPriority.SLOW);
    }

    private void pollByPriority(PollPriority priority) {
        // Find all ROW_HASH capable departments that are currently healthy
        List<String> activeDepts = adapters.values().stream()
            .filter(a -> a.getChangeDiscoveryCapability() == DepartmentAdapter.ChangeDiscoveryCapability.ROW_HASH
                      || a.getChangeDiscoveryCapability() == DepartmentAdapter.ChangeDiscoveryCapability.SNAPSHOT_DIFF)
            .filter(DepartmentAdapter::isHealthy)
            .map(DepartmentAdapter::getDepartmentCode)
            .toList();

        if (activeDepts.isEmpty()) {
            log.debug("[GhostPoller] No ROW_HASH departments to poll");
            return;
        }

        // Get all active shadow registry entries for these departments
        List<ShadowRegistryEntry> entries = shadowRegistry
            .findByDepartmentCodeInAndSyncStatus(activeDepts, ShadowRegistryEntry.SyncStatus.ACTIVE);

        // Filter by priority
        List<ShadowRegistryEntry> targetEntries = entries.stream()
            .filter(e -> getPriority(e) == priority)
            .toList();

        log.info("[GhostPoller] {} poll — checking {} records across {} departments",
                 priority, targetEntries.size(), activeDepts.size());

        for (ShadowRegistryEntry entry : targetEntries) {
            checkForChanges(entry);
        }
    }

    private void checkForChanges(ShadowRegistryEntry entry) {
        DepartmentAdapter adapter = adapters.get(entry.getDepartmentCode());
        if (adapter == null) {
            log.warn("[GhostPoller] No adapter found for dept={}", entry.getDepartmentCode());
            return;
        }

        Optional<UniversalBusinessRecord> changed =
            adapter.pollForChanges(entry.getLegacyId(), entry.getLastKnownHash());

        if (changed.isPresent()) {
            log.info("[GhostPoller] CHANGE DETECTED for UBID={} in dept={}",
                     entry.getUbid(), entry.getDepartmentCode());

            // Increment change counter for adaptive rate
            changeCounters.merge(entry.getLegacyId(), 1, Integer::sum);

            // Update shadow registry hash
            String newHash = computeHash(changed.get());
            shadowRegistry.updateHash(entry.getId(), newHash, Instant.now());

            // Fire the change event into the Kafka pipeline — same path as webhook events
            if(eventProducer!=null){
            eventProducer.publishDepartmentChangeEvent(changed.get(), 
                                                        entry.getDepartmentCode(),
                                                        "GHOST_POLL");
            }
        } else {
            // Decay the change counter over time for adaptive rate
            changeCounters.computeIfPresent(entry.getLegacyId(), 
                                             (k, v) -> Math.max(0, v - 1));
        }
    }

    private PollPriority getPriority(ShadowRegistryEntry entry) {
        int changes = changeCounters.getOrDefault(entry.getLegacyId(), 0);
        if (changes >= 5) return PollPriority.HIGH;

        // Records not synced in the last 7 days go to slow poll
        if (entry.getLastSyncedAt() != null) {
            long daysSinceSync = java.time.Duration.between(
                entry.getLastSyncedAt(), Instant.now()).toDays();
            if (daysSinceSync > 7) return PollPriority.SLOW;
        }
        return PollPriority.NORMAL;
    }

    private String computeHash(UniversalBusinessRecord record) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(record.toString().getBytes());
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) hex.append(String.format("%02x", b));
            return hex.toString();
        } catch (Exception e) {
            return "";
        }
    }

    public enum PollPriority { HIGH, NORMAL, SLOW }
}
