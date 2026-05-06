-- K-State Ledger (KSL) — Initial Schema
-- V1__initial_schema.sql

-- Shadow Registry
CREATE TABLE shadow_registry (
    id                   VARCHAR(36)  PRIMARY KEY,
    ubid                 VARCHAR(50)  NOT NULL,
    department_code      VARCHAR(30)  NOT NULL,
    legacy_id            VARCHAR(100) NOT NULL,
    legacy_id_field_name VARCHAR(50),
    system_endpoint      VARCHAR(500),
    sync_status          VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    last_known_hash      VARCHAR(64),
    last_synced_at       TIMESTAMP,
    created_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ubid_dept UNIQUE (ubid, department_code),
    CONSTRAINT chk_sync_status CHECK (sync_status IN ('ACTIVE','PENDING_SYNC','CONFLICT','STALE','INACTIVE'))
);
CREATE INDEX idx_sr_ubid        ON shadow_registry(ubid);
CREATE INDEX idx_sr_dept_code   ON shadow_registry(department_code);
CREATE INDEX idx_sr_sync_status ON shadow_registry(sync_status);

-- Audit Ledger (immutable)
CREATE TABLE audit_ledger (
    id                         VARCHAR(36)  PRIMARY KEY,
    correlation_id             VARCHAR(50)  NOT NULL,
    ubid                       VARCHAR(50)  NOT NULL,
    field_path                 VARCHAR(200),
    old_value                  TEXT,
    new_value                  TEXT,
    source_system              VARCHAR(30)  NOT NULL,
    source_event_id            VARCHAR(100),
    source_timestamp           TIMESTAMP,
    target_system              VARCHAR(30)  NOT NULL,
    target_record_id           VARCHAR(100),
    propagation_status         VARCHAR(20)  NOT NULL,
    retry_count                INT          NOT NULL DEFAULT 0,
    failure_reason             TEXT,
    conflict_detected          BOOLEAN      NOT NULL DEFAULT FALSE,
    conflict_resolution_policy VARCHAR(50),
    conflict_winner            VARCHAR(30),
    conflict_explanation       TEXT,
    rollback_payload           TEXT,
    recorded_at                TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_propagation_status CHECK (propagation_status IN (
        'SUCCESS','PENDING','RETRYING','FAILED','CONFLICT_HELD','ROLLED_BACK'))
);
CREATE INDEX idx_audit_ubid        ON audit_ledger(ubid);
CREATE INDEX idx_audit_correlation ON audit_ledger(correlation_id);
CREATE INDEX idx_audit_recorded_at ON audit_ledger(recorded_at);
CREATE INDEX idx_audit_status      ON audit_ledger(propagation_status);
CREATE INDEX idx_audit_conflict    ON audit_ledger(conflict_detected);

-- Processed Events Cache (idempotency)
CREATE TABLE processed_events (
    event_id     VARCHAR(100) PRIMARY KEY,
    processed_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ─────────────────────────────────────────────────────────────────────────────
-- SAMPLE DATA — 4 businesses across 5 departments
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO shadow_registry (id, ubid, department_code, legacy_id, legacy_id_field_name, system_endpoint, sync_status, last_synced_at) VALUES
-- Business 1: Acme Textiles — in SWS, Shops Est, Factories
('sr-001','KA-BIZ-2024-001','SWS',       'SWS-REG-78542',    'registration_id',      'http://localhost:8080/sws/api',        'ACTIVE', CURRENT_TIMESTAMP),
('sr-002','KA-BIZ-2024-001','SHOPS_EST', 'SE-2024-BLR-3341', 'se_registration_no',   'http://localhost:8081/shops-est/api', 'ACTIVE', CURRENT_TIMESTAMP),
('sr-003','KA-BIZ-2024-001','FACTORIES', 'FAC-KA-2024-0089', 'factory_licence_no',   'http://localhost:8082/factories/api', 'ACTIVE', CURRENT_TIMESTAMP),

-- Business 2: Green Earth Pvt Ltd — in SWS, Pollution Control
('sr-004','KA-BIZ-2024-002','SWS',            'SWS-REG-90111', 'registration_id',   'http://localhost:8080/sws/api',        'ACTIVE', CURRENT_TIMESTAMP),
('sr-005','KA-BIZ-2024-002','POLLUTION_CTRL', 'PCTB-2024-445', 'consent_order_no',  'http://localhost:8083/pctb/api',       'ACTIVE', CURRENT_TIMESTAMP),

-- Business 3: Karnataka Steel Works — in SWS, Factories, Labour Dept (stale)
('sr-006','KA-BIZ-2024-003','SWS',        'SWS-REG-55234',    'registration_id',    'http://localhost:8080/sws/api',        'ACTIVE', CURRENT_TIMESTAMP),
('sr-007','KA-BIZ-2024-003','FACTORIES',  'FAC-KA-2024-0201', 'factory_licence_no', 'http://localhost:8082/factories/api', 'STALE',  DATEADD(DAY, -10, CURRENT_TIMESTAMP)),
('sr-008','KA-BIZ-2024-003','LABOUR_DEPT','LD-2024-0789',      'labour_reg_no',      'http://localhost:8085/labour/api',    'ACTIVE', CURRENT_TIMESTAMP),

-- Business 4: Sunrise Hospitality — in SWS, Shops Est (conflict)
('sr-009','KA-BIZ-2024-004','SWS',       'SWS-REG-10044',    'registration_id',     'http://localhost:8080/sws/api',        'CONFLICT', CURRENT_TIMESTAMP),
('sr-010','KA-BIZ-2024-004','SHOPS_EST', 'SE-2024-MYS-0091', 'se_registration_no',  'http://localhost:8081/shops-est/api', 'CONFLICT', CURRENT_TIMESTAMP);

-- ─────────────────────────────────────────────────────────────────────────────
-- SAMPLE AUDIT ENTRIES — shows propagation history for demo
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO audit_ledger (id, correlation_id, ubid, field_path, old_value, new_value,
    source_system, source_event_id, target_system, target_record_id,
    propagation_status, conflict_detected, recorded_at) VALUES

-- Business 1: address change SWS → Shops Est (SUCCESS)
('al-001','corr-aaa-001','KA-BIZ-2024-001','registeredAddress.line1',
 '41 Brigade Road','42 Brigade Road','SWS','evt-sws-001','SHOPS_EST','SE-2024-BLR-3341',
 'SUCCESS', FALSE, DATEADD(MINUTE,-30,CURRENT_TIMESTAMP)),

-- Business 1: address change SWS → Factories (SUCCESS)
('al-002','corr-aaa-001','KA-BIZ-2024-001','registeredAddress.line1',
 '41 Brigade Road','42 Brigade Road','SWS','evt-sws-001','FACTORIES','FAC-KA-2024-0089',
 'SUCCESS', FALSE, DATEADD(MINUTE,-30,CURRENT_TIMESTAMP)),

-- Business 2: signatory change Factories → SWS via Ghost Poll (SUCCESS)
('al-003','corr-bbb-001','KA-BIZ-2024-001','authorisedSignatory.name',
 'Ramu Rao','Suresh Babu','FACTORIES','evt-ghost-001','SWS','SWS-REG-78542',
 'SUCCESS', FALSE, DATEADD(MINUTE,-15,CURRENT_TIMESTAMP)),

-- Business 4: CONFLICT — SWS and Shops Est updated phone simultaneously
('al-004','corr-ccc-001','KA-BIZ-2024-004','primaryPhone',
 '9900000000','9900000001','SWS','evt-sws-002','SHOPS_EST','SE-2024-MYS-0091',
 'CONFLICT_HELD', TRUE, DATEADD(MINUTE,-5,CURRENT_TIMESTAMP)),

-- Business 4: conflict audit record
('al-005','corr-ccc-002','KA-BIZ-2024-004','primaryPhone',
 '9900000000','9900000002','SHOPS_EST','evt-dept-001','SWS','SWS-REG-10044',
 'CONFLICT_HELD', TRUE, DATEADD(MINUTE,-5,CURRENT_TIMESTAMP)),

-- Business 3: FAILED propagation (stale dept)
('al-006','corr-ddd-001','KA-BIZ-2024-003','operationalStatus',
 'ACTIVE','SUSPENDED','SWS','evt-sws-003','FACTORIES','FAC-KA-2024-0201',
 'FAILED', FALSE, DATEADD(MINUTE,-2,CURRENT_TIMESTAMP));

-- Update conflict entries with explanation
UPDATE audit_ledger SET
    conflict_resolution_policy = 'SOURCE_HIERARCHY',
    conflict_winner = 'SWS',
    conflict_explanation = 'Field primaryPhone is under SWS authority per SOURCE_HIERARCHY policy. Update from SWS accepted; update from SHOPS_EST discarded. Conflict window: 30s.'
WHERE id IN ('al-004','al-005');
