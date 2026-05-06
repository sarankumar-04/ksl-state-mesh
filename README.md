# K-State Ledger (KSL) — Non-Invasive State Mesh
### Karnataka SWS ↔ Department Systems — Two-Way Interoperability

> **HackerEarth Submission · Theme 2 · AI for Bharat**
> Team: vishvasaran1 | Prototype Phase

---

## 🚀 Run in 2 Commands

```bash
cd ksl-core
mvn spring-boot:run -Dspring-boot.run.profiles=sandbox
```

Then open **http://localhost:8080** — the live dashboard opens automatically. No Kafka, no PostgreSQL, no login.

---

## What Opens

| URL | What |
|---|---|
| **http://localhost:8080** | Live KSL Dashboard (fully dynamic) |
| **http://localhost:8080/swagger-ui.html** | All REST APIs |
| **http://localhost:8080/h2-console** | Live database (JDBC: `jdbc:h2:mem:ksldb`, user: `ksl`, pass: `ksl`) |

---

## Demo Flow (for video/submission)

### 1. SWS → Departments (Direction 1)
- Open Dashboard → click **SWS → Departments** in sidebar
- Click **Fill Sample** → click **▶ Propagate**
- See the audit trail appear instantly with correlationId
- Check **Audit Ledger** → search `KA-BIZ-2024-001`

### 2. Department → SWS (Direction 2)
- Click **Dept → SWS Webhook**
- Fill Sample → Send Webhook
- Audit trail shows FACTORIES → SWS propagation

### 3. Conflict Simulation
- Click **Conflict Simulator**
- Fill Sample → **⚡ Trigger Conflict**
- Two simultaneous updates fire for same UBID
- Conflict detected, SOURCE_HIERARCHY policy applied
- Check **Conflicts** page — see the CONFLICT_HELD entry

### 4. Add a New Department
- Click **Departments** → **+ Add Department**
- Enter: Code=`FIRE_SAFETY`, UBID=`KA-BIZ-2024-001`, Legacy ID=`FS-2024-001`
- Instantly appears in registry and department grid

---

## Architecture

```
SWS ──webhook──▶ KSL Core ──fan-out──▶ Shops Est. (REST)
                    │                ──▶ Factories (DB View + Ghost Poll)
                    │                ──▶ Pollution Ctrl (Webhook)
                    │
Factories ──ghost-poll──▶ KSL Core ──normalise──▶ SWS
                    │
                    ├── Shadow Registry (UBID → LegacyID)
                    ├── Conflict Engine (SOURCE_HIERARCHY)
                    └── Immutable Audit Ledger
```

**7 Pillars:**
1. **Shadow Registry** — UBID→LegacyID golden record per department
2. **Universal Business Schema (UBS)** — common canonical model
3. **Adapter Pattern** — one class per department, zero source changes
4. **Ghost Poller** — SHA-256 hashing for event-less legacy systems
5. **Kafka Event Bus** — ordered by UBID partition key (disabled in sandbox)
6. **Conflict Engine** — SOURCE_HIERARCHY: SWS owns address/phone, Depts own signatory/license
7. **Immutable Audit Ledger** — append-only, every propagation traceable

---

## Prerequisites
- Java 17 or 21
- Maven 3.8+
- No other dependencies in sandbox mode

---

## GitHub Upload Steps

```bash
# 1. Create repo on github.com (name: ksl-state-mesh)
# 2. In this folder:
git init
git add .
git commit -m "KSL: K-State Ledger — Non-Invasive State Mesh for Karnataka SWS"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/ksl-state-mesh.git
git push -u origin main
```

---

## HackerEarth Submission Fields

| Field | Value |
|---|---|
| **Title** | K-State Ledger (KSL): The Non-Invasive State Mesh |
| **Demo Link** | http://localhost:8080 (or deployed URL) |
| **Repository URL** | https://github.com/YOUR_USERNAME/ksl-state-mesh |
| **Video URL** | Your Loom/YouTube demo link |
| **Theme** | Theme 2 — Two-Way Interoperability between SWS and Department Systems |
| **Instructions to Run** | `cd ksl-core && mvn spring-boot:run -Dspring-boot.run.profiles=sandbox` then open http://localhost:8080 |

---

## Sample API Calls (for testing)

```bash
# Stats
curl http://localhost:8080/api/v1/ksl/stats

# Trigger SWS propagation
curl -X POST http://localhost:8080/api/v1/ksl/propagate/sws \
  -H "Content-Type: application/json" \
  -d '{"ubid":"KA-BIZ-2024-001","businessName":"Acme Textiles","registeredAddress":{"line1":"42 Brigade Road","city":"Bengaluru","pincode":"560001"}}'

# Ingest department webhook  
curl -X POST http://localhost:8080/api/v1/ksl/webhook/FACTORIES \
  -H "Content-Type: application/json" \
  -d '{"ubid":"KA-BIZ-2024-001","authorisedSignatory":{"name":"Suresh Babu","designation":"Manager"}}'

# Simulate conflict
curl -X POST http://localhost:8080/api/v1/ksl/simulate/conflict \
  -H "Content-Type: application/json" \
  -d '{"ubid":"KA-BIZ-2024-004","primaryPhone":"9900000099"}'

# Audit trail
curl http://localhost:8080/api/v1/ksl/audit/KA-BIZ-2024-001

# Conflicts
curl http://localhost:8080/api/v1/ksl/audit/conflicts
```
