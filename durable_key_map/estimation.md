**Implementation Estimation – SIL Sensitive Data Pathway**

This estimate covers design finalization, build, testing, pilot, and operational handover for the column-isolation pattern (sensitive columns only in a tightly controlled lakehouse + OneLake shortcuts + Materialized Views). It assumes one pilot table (Sensitive PII) and the process workflow defined in the Solution Design Document.

### Assumptions
- Platform team has existing SIL patterns, Fabric pipelines/notebooks, and OneLake security experience.
- Source systems and join keys are stable and documented.
- No Sensitive Financial/PFI work is included.
- Estimates are for a small-to-medium team (1–2 platform engineers + supporting roles).
- Excludes long-term operational run costs after handover.

### Effort Estimate by Phase

| Phase | Activities | Estimated Effort | Calendar Time | Primary Roles |
|-------|------------|------------------|---------------|---------------|
| **1. Design Validation & Setup** | Finalize technical design, select pilot table, stand up Sensitive Pathway workspace/lakehouse, define Entra ID groups and OneLake role templates, document join-key strategy | 8–12 person-days | 1–2 weeks | Platform Engineer, Security/Compliance |
| **2. Build – Ingestion & Virtualization** | Column-only ingestion job (pipeline or notebook), OneLake Shortcuts, Materialized View (or scheduled Spark join), governance metadata/TBLPROPERTIES, error handling & logging | 12–18 person-days | 2–3 weeks | Platform Engineer |
| **3. Security & Access Controls** | OneLake data access roles (object-level only), workspace restrictions, Entra ID group assignment process, audit logging validation, disable/scope DefaultReader | 5–8 person-days | 1 week | Platform Engineer, Security |
| **4. Testing & UAT** | Isolation tests (sensitive columns absent from standard SIL), positive/negative access tests, refresh/latency tests, data quality checks, audit log verification | 8–12 person-days | 1.5–2 weeks | Platform Engineer, Domain pilot users, Security |
| **5. Documentation, Workflow & Handover** | Update runbooks, intake form/process, RACI operationalization, knowledge transfer, pilot retrospective | 4–6 person-days | 1 week | Platform Engineer, Data Governance |
| **Total** | | **37–56 person-days** | **~7–9 weeks** | |

### Pilot Timeline (Aligned with Document)
- **Weeks 1–2**: Phase 1 (Design Validation & Pilot Selection)
- **Weeks 3–5**: Phase 2 (Build & Configure)
- **Weeks 6–7**: Phase 3 (UAT & Security Testing)
- **Week 8**: Phase 4 (Go-Live & Handover)

### Cost Drivers & Risks
- **Higher effort** if join keys are missing/unstable, multiple source systems, or PHI requires additional compliance controls (audit retention, encryption verification).
- **Lower effort** if the pilot table is simple and existing SIL ingestion patterns can be reused with minimal changes.
- OneLake Shortcuts and Materialized Views are relatively low-complexity once the lakehouse isolation pattern is established; the majority of effort is in process, security, and validation.
- Post-pilot scaling to additional tables is estimated at **4–8 person-days per additional table** (mostly configuration and testing).

### Recommended Team
- 1 Platform Data Engineer (primary)
- 0.25–0.5 Security/Compliance (reviews and sign-off)
- 0.25 Data Governance (process and catalog)
- Domain pilot users for UAT

This estimate can be refined once the specific pilot table, source system, and exact join-key strategy are confirmed.
