| Component | Location | Responsibility |
| --- | --- | --- |
| Orchestration Workspace | Dedicated control workspace | Owns schedules, metadata lookup, and all cross-workspace triggering |
| domain_orchestration_pipeline | Orchestration Workspace | Master pipeline that reads the onboarding metadata and dynamically invokes targets |
| Onboarding Excel (domain_orchestration_pipeline tab) | Shared location (OneLake / SharePoint / Lakehouse) | Configuration source with Target Workspace ID and Target Pipeline ID columns |
| Domain Workspaces | WS1, WS2, WS3, … | Contain only the worker pipelines (they do not schedule themselves) |
| Shared Service Principal Connection | Fabric Connections | Used by every Invoke Pipeline activity so everything runs under the same identity |
