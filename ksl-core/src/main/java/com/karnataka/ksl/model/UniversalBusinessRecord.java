package com.karnataka.ksl.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import lombok.*;

import java.time.Instant;
import java.util.Map;

/**
 * Universal Business Schema (UBS)
 *
 * The common language across all department systems. Every adapter translates
 * its native representation INTO this schema before entering the KSL pipeline,
 * and translates FROM this schema when writing to a target system.
 *
 * This is schema-on-read: we don't force departments to adopt UBS — we translate
 * at the boundary. UBS lives entirely inside KSL.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UniversalBusinessRecord {

    /** The one true join key across all systems */
    @NotBlank
    private String ubid;

    // ── Core Identity ────────────────────────────────────────────────────────
    private String businessName;
    private String businessType;          // PROPRIETORSHIP, PARTNERSHIP, LLP, COMPANY, etc.
    private String panNumber;             // masked in LLM pipelines
    private String cinNumber;

    // ── Registered Address ───────────────────────────────────────────────────
    private Address registeredAddress;

    // ── Contact ──────────────────────────────────────────────────────────────
    private String primaryPhone;          // SWS is authoritative for this field
    private String primaryEmail;

    // ── Authorised Signatory ─────────────────────────────────────────────────
    private Signatory authorisedSignatory; // Department systems are authoritative

    // ── License & Status ─────────────────────────────────────────────────────
    private String operationalStatus;     // ACTIVE, SUSPENDED, CLOSED
    private Map<String, String> licenseNumbers; // dept-code → license-number

    // ── Metadata (set by KSL, not source systems) ────────────────────────────
    private String sourceSystem;          // SWS | SHOPS_EST | FACTORIES | POLLUTION_CTRL | ...
    private String sourceEventId;         // idempotency key from source
    private Instant recordedAt;
    private String schemaVersion;         // for forward-compatible evolution

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Address {
        private String line1;
        private String line2;
        private String city;
        private String district;
        private String state;
        private String pincode;
        private String taluk;
        private Double latitude;
        private Double longitude;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Signatory {
        private String name;
        private String designation;
        private String din;               // Director Identification Number
        private String phone;
        private String email;
    }
}
