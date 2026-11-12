**Solution Design Approach: Standardized CI/CD for Azure + Microsoft Fabric + Microsoft Foundry Agents**

This design establishes a single, repeatable, secure promotion path (DEV → non-production validation → PROD) for the multi-platform Teams Co-Pilot solution. The architecture components (Entra ID apps/identities, Teams personal tab, React/browser app, Azure App Service, Key Vault, Cosmos DB, Application Insights, Log Analytics, Foundry project + agents, Fabric Lakehouse/SQL endpoint, optional Power BI semantic model, optional lifecycle worker, networking/public gateway controls) are treated as versioned, parameterized artifacts under source control.

### 1. Guiding Principles
- **Git as single source of truth** for all deployable artifacts (Azure IaC, Fabric item definitions, Foundry agent definitions/prompts/code, application code).
- **Environment isolation**: Separate Azure subscriptions or resource groups, Fabric workspaces, and Foundry projects per environment. No shared mutable state.
- **Parameterization over hardcoding**: Environment-specific values (resource IDs, connection strings, lakehouse IDs, agent endpoints, Key Vault references) injected at deploy time.
- **Least-privilege automation**: Service principals / managed identities with minimal RBAC; secrets only in Key Vault; no long-lived secrets in pipelines.
- **Gates before promotion**: Automated validation + evaluation + manual approval (for non-prod → PROD).
- **Auditability & evidence**: Full pipeline logs, deployment history, approval records, test/evaluation reports, and tagged releases.
- **Rollback capability**: Redeploy previous Git commit/tag or previous Foundry agent version; Fabric items support unpublish/redeploy.
- **Minimal manual intervention**: Automated from validated merge to non-prod; controlled approval only for PROD.

### 2. Environment Model
| Environment | Azure | Fabric Workspace | Foundry Project | Purpose | Promotion Trigger |
|-------------|---------|------------------|-----------------|---------|-------------------|
| DEV | Dedicated RG/subscription | Git-connected DEV | DEV project | Development, unit tests, agent iteration | Auto on merge to `dev` |
| Non-prod (TEST/UAT) | Dedicated | Isolated TEST workspace | TEST project | Integration, data validation, agent evaluation, smoke tests | Automated after DEV success + optional approval |
| PROD | Dedicated | Isolated PROD workspace | PROD project | Production | Manual approval + production evidence gate |

Isolation extends to networking (private endpoints, NSGs, public gateway controls per environment) and Entra ID app registrations / managed identities.

### 3. Source Control & Branching Strategy
- Azure DevOps Git repository (or monorepo with clear folder structure).
- Recommended structure:
  ```
  /infra          # Bicep/Terraform for Azure resources (App Service, Key Vault, Cosmos, Foundry infra, networking, Entra)
  /app            # React/Teams tab, optional lifecycle worker
  /fabric         # Lakehouse, notebooks, pipelines, semantic model definitions (exported via Git integration or fabric-cicd format)
  /foundry        # Agent definitions, prompts, evaluation datasets, azure.yaml (for azd)
  /pipelines      # Azure Pipelines YAML
  /parameters     # parameter.yml (Fabric), env-specific variable files
  ```
- Branching: `main` (production-ready), `dev` (integration), feature branches. Squash merges preferred for clean history. Only the DEV Fabric workspace is Git-connected; higher environments receive deployments from the pipeline (not direct Git sync).

### 4. Deployment Mechanisms by Component
- **Azure resources** (App Service, Key Vault, Cosmos DB, App Insights, Log Analytics, networking, Entra ID apps/identities, Foundry project infrastructure):  
  Infrastructure as Code (prefer **Bicep** for Azure-native or **Terraform**). Deployed via `az deployment` or `terraform apply` in pipeline stages. Parameter files or variable groups supply environment-specific values. Use deployment stacks or resource locks where appropriate for PROD.

- **Microsoft Fabric artifacts** (Lakehouse + SQL endpoint, notebooks, pipelines, optional Power BI semantic model):  
  Primary tool: official **fabric-cicd** Python library (Microsoft-backed). It deploys item definitions from the repository directory, handles dependencies, supports parameterization via `parameter.yml` (replacing GUIDs, connection IDs, lakehouse references, etc.), and works with service principals.  
  Alternative/complement: Fabric Deployment Pipelines REST APIs for promotion between workspaces when Variable Libraries + deployment rules suffice.  
  Variable Libraries used for environment-specific configuration where supported. Data itself is never overwritten by metadata deployments.

- **Microsoft Foundry project and agents**:  
  Use **Azure Developer CLI (`azd`)** with the Foundry extension (`azd ai agent`) for infrastructure + agent version deployment, or direct Foundry APIs. Agents are versioned; promotion moves a validated agent version. Include evaluation datasets and quality gates (using Azure DevOps AI Agent Evaluation extension or `azd ai eval`). Separate Foundry projects per environment.

- **Application code** (React/Teams tab, optional lifecycle worker): Standard build (npm/yarn), artifact publish, and deploy to App Service (or container) with slot swaps or blue-green where needed.

### 5. CI/CD Pipeline Architecture (Azure Pipelines)
Multi-stage YAML pipeline with environments and approvals:

1. **Build & Validate** (triggered on PR/merge to `dev`/`main`):
   - Lint, unit tests, security scans (code, IaC, dependencies).
   - Fabric item validation, Foundry agent config validation.
   - Agent offline evaluation against datasets (quality thresholds as gates).
   - Publish build artifacts and test/evaluation reports.

2. **Deploy to DEV** (auto):
   - Deploy Azure IaC.
   - Deploy Fabric items via fabric-cicd (with DEV parameters).
   - Deploy/update Foundry agents (new version).
   - Deploy application.
   - Smoke tests.

3. **Deploy to Non-prod / Validation** (auto or light approval):
   - Same sequence with non-prod parameters.
   - Integration tests, data validation queries, full agent evaluations, end-to-end smoke tests (including Teams tab / React flows where feasible).
   - Collect evidence (test results, evaluation scores, deployment logs).

4. **Deploy to PROD** (manual approval required):
   - Approval gate in Azure DevOps Environment (with required reviewers, optional checks for work items/linked evidence).
   - Deploy using PROD parameters.
   - Post-deploy verification (smoke tests, health checks, Application Insights validation).
   - Generate production evidence package (pipeline run link, approval records, test reports, deployment notes, version tags).

Pipeline uses service connections (Workload Identity Federation preferred over secrets), variable groups linked to Key Vault, and environment-specific parameter files.

### 6. Security, Governance & Audit
- Service principals / managed identities with least-privilege roles (Fabric Contributor on target workspaces, appropriate Foundry and Azure roles). Separate identities per environment where practical.
- All secrets in Key Vault; pipeline authenticates via managed identity or federated credentials.
- Azure Policy / resource tags for environment, owner, solution name, and data classification.
- Networking isolation (private endpoints for Cosmos, Key Vault, Fabric where supported; public gateway controls as defined).
- Audit trail: Azure Pipelines logs + deployment history, Fabric deployment records, Foundry agent version history, Azure Activity Logs, and Application Insights / Log Analytics.
- Change management: Link pipeline runs to work items; require evidence attachment for PROD approvals.

### 7. Rollback & Recovery
- **Azure**: Redeploy previous IaC version from Git tag; App Service deployment slots or previous package.
- **Fabric**: Redeploy previous commit via fabric-cicd (or use deployment pipeline rollback where available); data remains intact.
- **Foundry agents**: Promote/roll back to a previous agent version.
- Documented runbook for each component; automated rollback stages can be added for critical paths.

### 8. Production Evidence & Compliance
Every PROD deployment produces:
- Immutable pipeline run ID and logs.
- Approval records (who, when, comments).
- Test/evaluation reports (pass/fail, scores).
- Deployed versions (Git SHA, Fabric item versions, Foundry agent version, app package version).
- Change summary and linked work items.
This package is stored as a pipeline artifact and can be retained per organizational retention policy.

### Recommended Next Steps in the Design
1. Confirm preferred IaC (Bicep vs Terraform) and exact Fabric item types in scope.
2. Define the concrete parameter schema (`parameter.yml` + variable groups).
3. Sketch the pipeline YAML stages and service principal RBAC matrix.
4. Identify which components need custom validation scripts (e.g., data quality checks on Lakehouse, agent evaluation thresholds).
5. Decide on Git-connected vs pure pipeline-driven Fabric workspaces for non-prod/PROD.

This approach delivers consistent, auditable, low-touch promotion while preserving isolation and the ability to roll back. It leverages Microsoft-supported tooling (fabric-cicd, azd Foundry extension, Azure Pipelines, Bicep/Terraform) so the solution remains maintainable as the platforms evolve.

I can next detail the repository structure, pipeline YAML skeleton, parameter file examples, RBAC matrix, or the specific deployment sequence for Fabric + Foundry. Which part of the design would you like to expand first?
