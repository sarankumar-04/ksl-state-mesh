package com.karnataka.ksl.adapter;

import com.karnataka.ksl.model.UniversalBusinessRecord;
import com.karnataka.ksl.repository.ShadowRegistryRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Shops & Establishments Department Adapter
 *
 * This department exposes a REST API (JSON). The adapter handles:
 *   - Fetching records by their internal "se_registration_no"
 *   - Translating ShopsEstRecord ↔ UniversalBusinessRecord
 *   - Idempotent writes via the department's POST /registrations/{id}/update endpoint
 *   - Change polling via a "modified_after" query param
 *
 * Circuit breaker: if the SE API is down, we back off and queue for retry.
 * We NEVER modify the SE system's schema or add triggers — only use existing endpoints.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ShopsEstablishmentsAdapter implements DepartmentAdapter {

    private final RestTemplate restTemplate;
    private final ShadowRegistryRepository shadowRegistry;

    private static final String DEPT_CODE = "SHOPS_EST";
    // In real deployment, injected from application.yml
    private static final String BASE_URL = "${ksl.adapters.shops-est.base-url:http://localhost:8081/shops-est/api}";

    @Override
    public String getDepartmentCode() { return DEPT_CODE; }

    @Override
    public String getDepartmentName() { return "Shops and Commercial Establishments"; }

    @Override
    public ChangeDiscoveryCapability getChangeDiscoveryCapability() {
        return ChangeDiscoveryCapability.API_EVENTS; // This dept has a modified_after API
    }

    @Override
    @CircuitBreaker(name = "shops-est", fallbackMethod = "fetchFallback")
    public Optional<UniversalBusinessRecord> fetchByLegacyId(String legacyId) {
        try {
            log.debug("[SHOPS_EST] Fetching record for legacyId={}", legacyId);
            ShopsEstRecord raw = restTemplate.getForObject(
                BASE_URL + "/registrations/" + legacyId, ShopsEstRecord.class);
            if (raw == null) return Optional.empty();
            return Optional.of(translateToUbs(raw));
        } catch (Exception e) {
            log.warn("[SHOPS_EST] Fetch failed for legacyId={}: {}", legacyId, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public Optional<UniversalBusinessRecord> fetchByUbid(String ubid) {
        return shadowRegistry.findByUbidAndDepartmentCode(ubid, DEPT_CODE)
            .flatMap(entry -> fetchByLegacyId(entry.getLegacyId()));
    }

    @Override
    @CircuitBreaker(name = "shops-est", fallbackMethod = "writeFallback")
    public WriteResult write(UniversalBusinessRecord ubsRecord, String idempotencyKey) {
        log.info("[SHOPS_EST] Writing record for UBID={}, idempotencyKey={}", 
                 ubsRecord.getUbid(), idempotencyKey);

        // Step 1: resolve UBID → legacy ID
        var entry = shadowRegistry.findByUbidAndDepartmentCode(ubsRecord.getUbid(), DEPT_CODE);
        if (entry.isEmpty()) {
            return WriteResult.failure("No shadow registry entry for UBID " + ubsRecord.getUbid());
        }

        // Step 2: translate UBS → department native schema
        ShopsEstUpdatePayload payload = translateFromUbs(ubsRecord);
        payload.setIdempotencyKey(idempotencyKey); // Pass to dept system for dedup

        // Step 3: POST to department API
        try {
            restTemplate.put(
                BASE_URL + "/registrations/" + entry.get().getLegacyId() + "/update",
                payload);
            log.info("[SHOPS_EST] Write SUCCESS for UBID={}", ubsRecord.getUbid());
            return WriteResult.success(entry.get().getLegacyId(), idempotencyKey);
        } catch (Exception e) {
            log.error("[SHOPS_EST] Write FAILED for UBID={}: {}", ubsRecord.getUbid(), e.getMessage());
            return WriteResult.failure(e.getMessage());
        }
    }

    @Override
    public Optional<UniversalBusinessRecord> pollForChanges(String legacyId, String lastKnownHash) {
        Optional<UniversalBusinessRecord> current = fetchByLegacyId(legacyId);
        if (current.isEmpty()) return Optional.empty();

        String currentHash = computeHash(current.get());
        if (currentHash.equals(lastKnownHash)) {
            return Optional.empty(); // No change
        }
        log.info("[SHOPS_EST] Hash mismatch for legacyId={}, change detected", legacyId);
        return current;
    }

    @Override
    public List<UniversalBusinessRecord> fetchChangedSince(Instant since) {
        log.debug("[SHOPS_EST] Fetching all changes since {}", since);
        try {
            ShopsEstRecord[] changed = restTemplate.getForObject(
                BASE_URL + "/registrations?modified_after=" + since.toEpochMilli(),
                ShopsEstRecord[].class);
            if (changed == null) return List.of();
            return java.util.Arrays.stream(changed)
                .map(this::translateToUbs)
                .toList();
        } catch (Exception e) {
            log.error("[SHOPS_EST] fetchChangedSince failed: {}", e.getMessage());
            return List.of();
        }
    }

    @Override
    public boolean isHealthy() {
        try {
            restTemplate.headForHeaders(BASE_URL + "/health");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ── Schema Translation ────────────────────────────────────────────────────

    private UniversalBusinessRecord translateToUbs(ShopsEstRecord raw) {
        return UniversalBusinessRecord.builder()
            .ubid(raw.getUbid())
            .businessName(raw.getEstablishment_name())
            .businessType(mapBusinessType(raw.getEstablishment_type()))
            .primaryPhone(raw.getContact_mobile())
            .primaryEmail(raw.getContact_email())
            .registeredAddress(UniversalBusinessRecord.Address.builder()
                .line1(raw.getAddress_line1())
                .line2(raw.getAddress_line2())
                .city(raw.getCity())
                .district(raw.getDistrict())
                .pincode(raw.getPincode())
                .build())
            .authorisedSignatory(UniversalBusinessRecord.Signatory.builder()
                .name(raw.getOwner_name())
                .designation(raw.getOwner_designation())
                .build())
            .sourceSystem(DEPT_CODE)
            .recordedAt(Instant.now())
            .schemaVersion("1.0")
            .build();
    }

    private ShopsEstUpdatePayload translateFromUbs(UniversalBusinessRecord ubs) {
        ShopsEstUpdatePayload p = new ShopsEstUpdatePayload();
        if (ubs.getRegisteredAddress() != null) {
            p.setAddress_line1(ubs.getRegisteredAddress().getLine1());
            p.setAddress_line2(ubs.getRegisteredAddress().getLine2());
            p.setCity(ubs.getRegisteredAddress().getCity());
            p.setDistrict(ubs.getRegisteredAddress().getDistrict());
            p.setPincode(ubs.getRegisteredAddress().getPincode());
        }
        if (ubs.getAuthorisedSignatory() != null) {
            p.setOwner_name(ubs.getAuthorisedSignatory().getName());
        }
        p.setContact_mobile(ubs.getPrimaryPhone());
        p.setContact_email(ubs.getPrimaryEmail());
        return p;
    }

    private String mapBusinessType(String deptType) {
        return switch (deptType != null ? deptType.toUpperCase() : "") {
            case "SOLE_PROP", "PROPRIETOR" -> "PROPRIETORSHIP";
            case "PARTNERSHIP_FIRM" -> "PARTNERSHIP";
            case "PVT_LTD", "LTD" -> "COMPANY";
            default -> deptType;
        };
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

    // Fallback methods for circuit breaker
    public Optional<UniversalBusinessRecord> fetchFallback(String legacyId, Exception e) {
        log.warn("[SHOPS_EST] Circuit breaker OPEN — fetch fallback for legacyId={}", legacyId);
        return Optional.empty();
    }

    public WriteResult writeFallback(UniversalBusinessRecord record, String key, Exception e) {
        log.warn("[SHOPS_EST] Circuit breaker OPEN — write queued for UBID={}", record.getUbid());
        return WriteResult.failure("Circuit breaker open: " + e.getMessage());
    }

    // ── Inner DTOs (department's native schema) ───────────────────────────────

    @lombok.Data
    public static class ShopsEstRecord {
        private String ubid;
        private String se_registration_no;
        private String establishment_name;
        private String establishment_type;
        private String address_line1;
        private String address_line2;
        private String city;
        private String district;
        private String pincode;
        private String contact_mobile;
        private String contact_email;
        private String owner_name;
        private String owner_designation;
        private String status;
    }

    @lombok.Data
    public static class ShopsEstUpdatePayload {
        private String address_line1;
        private String address_line2;
        private String city;
        private String district;
        private String pincode;
        private String contact_mobile;
        private String contact_email;
        private String owner_name;
        private String idempotencyKey;
    }
}
