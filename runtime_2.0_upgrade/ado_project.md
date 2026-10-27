**Fabric Spark Runtime 1.3 → 2.0 Upgrade Plan**  
*(Azure DevOps style – Epic + User Stories + Tasks with estimates)*

**Epic:** Migrate custom Python package/wheel to Fabric Spark Runtime 2.0 (Python 3.13, Spark 4.1, Delta 4.2, Azure Linux 3.0)  
**Priority:** High  
**Target Window:** 4–6 weeks (can be compressed for pure-Python packages)  
**Success Criteria:** Wheel installs and runs cleanly in Runtime 2.0 Environment; regression suite passes; production notebooks/jobs can be switched with rollback path intact.

### Feature 1: Assessment & Inventory
**User Story:** As a data engineer, I need a complete inventory of the package, dependencies, and runtime usage so I can quantify the migration effort.  
**Story Points:** 5

| Task | Description | Estimate | Acceptance Criteria |
|------|-------------|----------|---------------------|
| 1.1 | Inventory package type (pure Python vs native/Cython), current Python version, and all dependencies | 2h | List of dependencies with current pins + notes on native extensions |
| 1.2 | Compare Runtime 1.3 vs 2.0 preinstalled packages (use Fabric runtime GitHub inventories) | 3h | Gap list of packages that must now be added explicitly |
| 1.3 | Identify PySpark / pandas API on Spark usage and potential breaking changes | 3h | Flagged code locations (iteritems, append, inplace, etc.) |
| 1.4 | Document current wheel build process and CI pipeline | 2h | Build process documented |

### Feature 2: Code & Dependency Compatibility
**User Story:** As a developer, I need the package and its dependencies to be compatible with Python 3.13 and Spark 4.1 so the wheel can be installed in Runtime 2.0.  
**Story Points:** 8–13 (higher if native extensions or heavy pandas API usage)

| Task | Description | Estimate | Acceptance Criteria |
|------|-------------|----------|---------------------|
| 2.1 | Update `python_requires` and dependency pins to versions with cp313 / manylinux wheels | 4h | `pyproject.toml` / `setup.py` updated; dry-run install succeeds on Python 3.13 |
| 2.2 | Fix Python 3.13 and PySpark API incompatibilities | 8–16h | No deprecation warnings or runtime errors on 3.13 + Spark 4.1 patterns |
| 2.3 | Add any missing packages that were preinstalled in 1.3 but removed in 2.0 | 3h | Explicit list ready for Environment |
| 2.4 | Update unit tests to run under Python 3.13 | 4h | Test suite green on 3.13 locally |

### Feature 3: Wheel Rebuild & Packaging
**User Story:** As a release engineer, I need a new wheel built for Python 3.13 / Azure Linux 3.0 so it can be uploaded as a custom library.  
**Story Points:** 5–8

| Task | Description | Estimate | Acceptance Criteria |
|------|-------------|----------|---------------------|
| 3.1 | Set up Python 3.13 build environment (local, WSL, or CI) | 3h | `python --version` = 3.13 and build tools present |
| 3.2 | Rebuild wheel (use cibuildwheel for manylinux_2_17_x86_64 if native extensions) | 4–8h | Wheel filename contains `cp313` (or pure-py3) and is Linux-compatible |
| 3.3 | Validate wheel metadata and basic import | 2h | `pip install` + import succeeds outside Fabric |
| 3.4 | Update CI pipeline to produce 3.13 wheel (optional parallel 3.11 if dual support desired) | 4h | Pipeline produces correct artifact |

### Feature 4: Fabric Environment Validation (Staged)
**User Story:** As a platform engineer, I need to validate the new wheel in an isolated Runtime 2.0 Environment before touching production.  
**Story Points:** 8

| Task | Description | Estimate | Acceptance Criteria |
|------|-------------|----------|---------------------|
| 4.1 | Create dedicated Runtime 2.0 Environment and upload new wheel + missing packages | 3h | Environment publishes successfully |
| 4.2 | Attach Environment to copies of representative notebooks / Spark jobs | 2h | Sessions start on Runtime 2.0 |
| 4.3 | Execute regression suite (schemas, row counts, aggregates, critical transforms) | 8–12h | No functional differences vs Runtime 1.3 baseline |
| 4.4 | Validate downstream consumers (SQL endpoint, Power BI, pipelines) | 4h | Tables readable; no protocol surprises |
| 4.5 | Document rollback path (keep 1.3 Environment) | 2h | Rollback procedure written |

### Feature 5: Production Rollout & Cleanup
**User Story:** As a release manager, I need a controlled production rollout with monitoring so we can switch workloads safely.  
**Story Points:** 5

| Task | Description | Estimate | Acceptance Criteria |
|------|-------------|----------|---------------------|
| 5.1 | Pilot: Attach Runtime 2.0 Environment to a small set of non-critical notebooks/jobs | 3h | Pilot runs cleanly for 1 week |
| 5.2 | Expand to remaining workloads in batches | 4h | All target items on 2.0 |
| 5.3 | Update workspace default runtime (after item-level migration) | 1h | New sessions use 2.0 |
| 5.4 | Retire or archive Runtime 1.3 Environment after observation period | 2h | Cleanup complete; documentation updated |

### Summary Estimates
| Feature | Story Points | Rough Calendar Time |
|---------|--------------|---------------------|
| Assessment & Inventory | 5 | 2–3 days |
| Code & Dependency Compatibility | 8–13 | 1–2 weeks |
| Wheel Rebuild & Packaging | 5–8 | 3–5 days |
| Fabric Validation | 8 | 1 week |
| Production Rollout | 5 | 1–2 weeks (including observation) |
| **Total** | **31–39** | **4–6 weeks** |

### Key Risks & Mitigations
- **Native extensions fail to build for Azure Linux 3.0** → Use cibuildwheel + manylinux_2_17; test early.
- **Dependency has no Python 3.13 wheel** → Pin alternative version, fork, or replace package.
- **Delta protocol bump breaks other engines** → Do not enable new Delta 4.x features during migration; verify table metadata.
- **Unexpected Spark 4.1 behavior changes** → Run side-by-side regression before production switch.
- **Library publish fails in Environment** → Resolve conflicts in Library Manager; prefer explicit pins.

### Recommended ADO Structure
- One Epic
- Five Features (or keep as User Stories under the Epic)
- Tasks as above (can be further broken if needed)
- Link related PRs, notebook copies, and Environment items
- Add a “Definition of Done” checklist to the Epic: wheel published, regression green, rollback documented, production pilot completed

This plan is deliberately staged so production remains on Runtime 1.3 until validation is complete. Adjust story points upward if the package has heavy native code or complex Spark UDFs.
