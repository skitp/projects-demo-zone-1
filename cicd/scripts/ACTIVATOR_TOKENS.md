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
1. Place the tokenized `ReflexEntities.json` (see `ReflexEntities.tokenized.json`) in your `*.Reflex` folder.
2. Set the above variables in your Azure DevOps variable group or as pipeline variables.
3. The `Invoke-ActivatorManagement.ps1` script automatically replaces `{{TOKEN}}` with the matching environment variable before deploying.

## Example SUB_ARTIFACT_DISPLAY_NAME
```
/subscriptions/{{SUBSCRIPTION_ID}}/resourceGroups/{{RESOURCE_GROUP}}/providers/microsoft.storage/storageaccounts/{{STORAGE_ACCOUNT_NAME}}
```
(You can nest tokens; the script performs a single-pass replacement of all `{{...}}`.)
