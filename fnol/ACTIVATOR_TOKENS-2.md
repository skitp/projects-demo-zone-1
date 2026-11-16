# Recommended pipeline / environment variables for Activator token substitution

| Token                      | Description                                      | Example / Source                  |
|----------------------------|--------------------------------------------------|-----------------------------------|
| `STORAGE_ACCOUNT_NAME`     | Storage account name                             | `bhgdevdnafnolaieussa`            |
| `TENANT_ID`                | Azure AD / Entra tenant ID                       | From variable group               |
| `WORKSPACE_ID`             | Fabric workspace ID                              | Resolved in pipeline              |
| `ARTIFACT_ID`              | Eventstream / Real-Time Hub artifact ID          | Environment-specific              |
| `SUB_ARTIFACT_ID`          | Sub-artifact ID for the storage connection       | Environment-specific              |
| `SUB_ARTIFACT_DISPLAY_NAME`| Full ARM resource path of the storage account    | Constructed or from Key Vault     |
| `NOTEBOOK_ITEM_ID`         | Target Synapse Notebook item ID to invoke        | Environment-specific              |

## Usage
1. Place the tokenized template in `DevOpsServices/pipelines/templates/`  
   (recommended names: `{DisplayName}.ReflexEntities.json` or `{FolderName}.ReflexEntities.json`).
2. Keep the official `*.Reflex` folder (managed by Fabric Git integration) clean — do **not** put tokens inside it.
3. Set the tokens as Azure DevOps variables / environment variables.
4. The `Invoke-ActivatorManagement.ps1` script prefers the template from `DevOpsServices/pipelines/templates/` and performs substitution **in memory only**.

## Example SUB_ARTIFACT_DISPLAY_NAME
```
/subscriptions/{{SUBSCRIPTION_ID}}/resourceGroups/{{RESOURCE_GROUP}}/providers/microsoft.storage/storageaccounts/{{STORAGE_ACCOUNT_NAME}}
```
(You can nest tokens; the script performs a single-pass replacement of all `{{...}}`.)
