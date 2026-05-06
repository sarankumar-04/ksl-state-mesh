package com.karnataka.ksl.adapter;

import com.karnataka.ksl.model.UniversalBusinessRecord;
import com.karnataka.ksl.repository.ShadowRegistryRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Factories Department Adapter — Database View + Ghost Poll
 *
 * The Factories system exposes a read-only SQL view (no webhook, no REST events).
 * KSL reads via JDBC and the Ghost Poller hashes each row to detect changes.
 * Writes go through a stored procedure (the only write surface the Factories
 * team agreed to expose).
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class FactoriesAdapter implements DepartmentAdapter {

    private final JdbcTemplate jdbcTemplate;
    private final ShadowRegistryRepository shadowRegistry;

    @Override public String getDepartmentCode() { return "FACTORIES"; }
    @Override public String getDepartmentName() { return "Factories Department (Karnataka)"; }
    @Override public ChangeDiscoveryCapability getChangeDiscoveryCapability() {
        return ChangeDiscoveryCapability.ROW_HASH; // no events — Ghost Poll only
    }

    @Override
    @CircuitBreaker(name = "factories", fallbackMethod = "fetchFallback")
    public Optional<UniversalBusinessRecord> fetchByLegacyId(String legacyId) {
        try {
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT * FROM v_ksl_factories WHERE factory_licence_no = ?", legacyId);
            if (rows.isEmpty()) return Optional.empty();
            return Optional.of(rowToUbs(rows.get(0)));
        } catch (Exception e) {
            log.warn("[FACTORIES] Fetch failed for legacyId={}: {}", legacyId, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public Optional<UniversalBusinessRecord> fetchByUbid(String ubid) {
        return shadowRegistry.findByUbidAndDepartmentCode(ubid, "FACTORIES")
            .flatMap(e -> fetchByLegacyId(e.getLegacyId()));
    }

    @Override
    @CircuitBreaker(name = "factories", fallbackMethod = "writeFallback")
    public WriteResult write(UniversalBusinessRecord record, String idempotencyKey) {
        var entry = shadowRegistry.findByUbidAndDepartmentCode(record.getUbid(), "FACTORIES");
        if (entry.isEmpty()) return WriteResult.failure("No shadow entry for UBID");
        try {
            // Factories exposes a stored procedure for updates
            jdbcTemplate.update(
                "CALL ksl_update_factory(?, ?, ?, ?, ?)",
                entry.get().getLegacyId(),
                record.getRegisteredAddress() != null ? record.getRegisteredAddress().getLine1() : null,
                record.getRegisteredAddress() != null ? record.getRegisteredAddress().getPincode() : null,
                record.getAuthorisedSignatory() != null ? record.getAuthorisedSignatory().getName() : null,
                idempotencyKey
            );
            return WriteResult.success(entry.get().getLegacyId(), idempotencyKey);
        } catch (Exception e) {
            return WriteResult.failure(e.getMessage());
        }
    }

    @Override
    public Optional<UniversalBusinessRecord> pollForChanges(String legacyId, String lastKnownHash) {
        Optional<UniversalBusinessRecord> current = fetchByLegacyId(legacyId);
        if (current.isEmpty()) return Optional.empty();
        String currentHash = computeHash(current.get());
        if (currentHash.equals(lastKnownHash)) return Optional.empty();
        return current;
    }

    @Override
    public List<UniversalBusinessRecord> fetchChangedSince(Instant since) { return List.of(); }

    @Override
    public boolean isHealthy() {
        try { jdbcTemplate.queryForObject("SELECT 1 FROM v_ksl_factories LIMIT 1", Integer.class); return true; }
        catch (Exception e) { return false; }
    }

    private UniversalBusinessRecord rowToUbs(Map<String, Object> row) {
        return UniversalBusinessRecord.builder()
            .ubid((String) row.get("ubid"))
            .businessName((String) row.get("factory_name"))
            .registeredAddress(UniversalBusinessRecord.Address.builder()
                .line1((String) row.get("address")).pincode((String) row.get("pincode")).build())
            .authorisedSignatory(UniversalBusinessRecord.Signatory.builder()
                .name((String) row.get("authorised_person")).build())
            .operationalStatus((String) row.get("license_status"))
            .sourceSystem("FACTORIES").recordedAt(Instant.now()).schemaVersion("1.0").build();
    }

    private String computeHash(UniversalBusinessRecord r) {
        try {
            byte[] h = MessageDigest.getInstance("SHA-256").digest(r.toString().getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : h) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { return ""; }
    }

    public Optional<UniversalBusinessRecord> fetchFallback(String id, Exception e) { return Optional.empty(); }
    public WriteResult writeFallback(UniversalBusinessRecord r, String k, Exception e) {
        return WriteResult.failure("Circuit breaker open");
    }
}
