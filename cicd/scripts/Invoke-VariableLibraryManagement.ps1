<#
.SYNOPSIS
    Creates or updates a Microsoft Fabric Variable Library item.

.DESCRIPTION
    Production-ready management script for Fabric Variable Libraries.
    Follows the same patterns as Invoke-NotebookManagement.ps1 / Invoke-EnvironmentManagement.ps1.

    Supports:
    - CreateOrUpdate (primary)
    - Automatic detection of existing item by displayName
    - Definition from folder (.platform + variables.json + settings.json + valueSets/*)
    - Pre-built definition JSON
    - Folder placement via FolderHierarchy + FolderPath
    - Long-running operation (LRO) polling
    - Azure DevOps friendly logging

.PARAMETER Action
    CreateOrUpdate | Get | Delete

.PARAMETER WorkspaceId
    Target workspace GUID.

.PARAMETER DisplayName
    Display name of the Variable Library.

.PARAMETER Description
    Optional description (max 256 characters).

.PARAMETER FolderHierarchy
    JSON string produced by Fabric-FolderSynchronization.ps1 (optional).

.PARAMETER FolderPath
    Relative path under src/fabric (used to resolve folderId).

.PARAMETER PlatformFile
    Full path to the .platform file. When provided the script will also look for
    variables.json, settings.json and valueSets/ next to it.

.PARAMETER DefinitionJson
    Pre-built VariableLibraryPublicDefinition as a JSON string.
    Takes precedence over PlatformFile when both are supplied.

.PARAMETER UpdateMetadata
    When updating, also apply metadata from the .platform file (default: $true).
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('CreateOrUpdate', 'Get', 'Delete')]
    [string]$Action,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $true)]
    [string]$DisplayName,

    [Parameter(Mandatory = $false)]
    [string]$Description = $null,

    [Parameter(Mandatory = $false)]
    [string]$FolderHierarchy = $null,

    [Parameter(Mandatory = $false)]
    [string]$FolderPath = $null,

    [Parameter(Mandatory = $false)]
    [string]$PlatformFile = $null,

    [Parameter(Mandatory = $false)]
    [string]$DefinitionJson = $null,

    [Parameter(Mandatory = $false)]
    [bool]$UpdateMetadata = $true
)

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

function Write-Section { param([string]$Message) Write-Host "##[section]$Message" }
function Write-DebugLog { param([string]$Message) Write-Host "##[debug]$Message" }
function Write-WarningLog { param([string]$Message) Write-Host "##[warning]$Message" }
function Write-ErrorLog { param([string]$Message) Write-Host "##[error]$Message" }

function Get-FabricHeaders {
    $token = $env:FABRIC_TOKEN
    if ([string]::IsNullOrWhiteSpace($token)) {
        throw "FABRIC_TOKEN environment variable is not set. Authenticate before calling this script."
    }
    return @{
        "Authorization" = "Bearer $token"
        "Content-Type"  = "application/json"
    }
}

function Invoke-FabricRest {
    param(
        [string]$Method,
        [string]$Uri,
        [hashtable]$Headers,
        [object]$Body = $null,
        [int]$TimeoutSec = 120
    )

    $params = @{
        Method      = $Method
        Uri         = $Uri
        Headers     = $Headers
        TimeoutSec  = $TimeoutSec
        ErrorAction = 'Stop'
    }

    if ($null -ne $Body) {
        $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 20 -Compress }
    }

    try {
        $response = Invoke-WebRequest @params -UseBasicParsing
        return $response
    }
    catch {
        $statusCode = $_.Exception.Response.StatusCode.value__
        $errorBody  = $null
        try {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $errorBody = $reader.ReadToEnd()
            $reader.Close()
        } catch {}

        Write-ErrorLog "HTTP $statusCode calling $Method $Uri"
        if ($errorBody) { Write-ErrorLog "Response: $errorBody" }
        throw
    }
}

function Wait-FabricLro {
    param(
        $Response,
        [hashtable]$Headers,
        [int]$MaxWaitSeconds = 300,
        [int]$PollIntervalSeconds = 5
    )

    if ($Response.StatusCode -ne 202) {
        return $null
    }

    $operationUrl = $Response.Headers['Location']
    if ([string]::IsNullOrWhiteSpace($operationUrl)) {
        $operationUrl = $Response.Headers['Operation-Location']
    }

    if ([string]::IsNullOrWhiteSpace($operationUrl)) {
        Write-WarningLog "202 Accepted received but no Location / Operation-Location header found."
        return $null
    }

    Write-DebugLog "LRO started. Polling: $operationUrl"
    $elapsed = 0

    while ($elapsed -lt $MaxWaitSeconds) {
        Start-Sleep -Seconds $PollIntervalSeconds
        $elapsed += $PollIntervalSeconds

        try {
            $statusResp = Invoke-RestMethod -Uri $operationUrl -Headers $Headers -Method GET -ErrorAction Stop
            $status = $statusResp.status

            Write-DebugLog "LRO status: $status (elapsed ${elapsed}s)"

            if ($status -eq 'Succeeded' -or $status -eq 'Completed') {
                if ($statusResp.PSObject.Properties['result']) {
                    return $statusResp.result
                }
                if ($statusResp.PSObject.Properties['resourceId']) {
                    return @{ id = $statusResp.resourceId }
                }
                return $statusResp
            }
            elseif ($status -eq 'Failed' -or $status -eq 'Canceled') {
                $errorMsg = if ($statusResp.error) { $statusResp.error | ConvertTo-Json -Compress } else { "Unknown error" }
                throw "Long-running operation failed: $errorMsg"
            }
        }
        catch {
            Write-WarningLog "Error while polling LRO: $($_.Exception.Message)"
        }
    }

    throw "LRO timed out after $MaxWaitSeconds seconds."
}

function Resolve-FolderId {
    param(
        [string]$FolderHierarchyJson,
        [string]$RelativeFolderPath
    )

    if ([string]::IsNullOrWhiteSpace($FolderHierarchyJson) -or [string]::IsNullOrWhiteSpace($RelativeFolderPath)) {
        return $null
    }

    try {
        $hierarchy = $FolderHierarchyJson | ConvertFrom-Json
        $normalized = $RelativeFolderPath.Replace('\', '/').Trim('/').ToLowerInvariant()

        if ($hierarchy -is [PSCustomObject] -or $hierarchy -is [hashtable]) {
            $props = $hierarchy.PSObject.Properties
            foreach ($p in $props) {
                $key = $p.Name.Replace('\', '/').Trim('/').ToLowerInvariant()
                if ($key -eq $normalized -or $key.EndsWith("/$normalized")) {
                    Write-DebugLog "Resolved folderId '$($p.Value)' for path '$RelativeFolderPath'"
                    return $p.Value
                }
            }
        }

        # Nested walk fallback
        $current = $hierarchy
        $segments = $normalized.Split('/') | Where-Object { $_ }
        foreach ($seg in $segments) {
            if ($null -eq $current) { break }
            $child = $null
            if ($current.PSObject.Properties['children']) {
                $child = $current.children | Where-Object {
                    $_.displayName -and $_.displayName.ToLowerInvariant() -eq $seg
                } | Select-Object -First 1
            }
            elseif ($current.PSObject.Properties[$seg]) {
                $child = $current.$seg
            }
            $current = $child
        }

        if ($current -and $current.id) {
            Write-DebugLog "Resolved folderId '$($current.id)' via nested walk for path '$RelativeFolderPath'"
            return $current.id
        }
    }
    catch {
        Write-WarningLog "Could not resolve folderId from hierarchy: $($_.Exception.Message)"
    }

    return $null
}

function Build-DefinitionFromFolder {
    param(
        [string]$PlatformFilePath
    )

    if (-not (Test-Path $PlatformFilePath)) {
        throw "Platform file not found: $PlatformFilePath"
    }

    $dir = Split-Path -Parent $PlatformFilePath
    $parts = @()

    # .platform
    $bytes = [System.IO.File]::ReadAllBytes($PlatformFilePath)
    $parts += @{
        path        = ".platform"
        payload     = [Convert]::ToBase64String($bytes)
        payloadType = "InlineBase64"
    }

    # variables.json
    $varFile = Join-Path $dir "variables.json"
    if (Test-Path $varFile) {
        $bytes = [System.IO.File]::ReadAllBytes($varFile)
        $parts += @{
            path        = "variables.json"
            payload     = [Convert]::ToBase64String($bytes)
            payloadType = "InlineBase64"
        }
    }
    else {
        Write-WarningLog "variables.json not found next to platform file – library will be created empty"
    }

    # settings.json
    $settingsFile = Join-Path $dir "settings.json"
    if (Test-Path $settingsFile) {
        $bytes = [System.IO.File]::ReadAllBytes($settingsFile)
        $parts += @{
            path        = "settings.json"
            payload     = [Convert]::ToBase64String($bytes)
            payloadType = "InlineBase64"
        }
    }

    # valueSets/*.json
    $valueSetsDir = Join-Path $dir "valueSets"
    if (Test-Path $valueSetsDir) {
        Get-ChildItem -Path $valueSetsDir -Filter "*.json" -File | ForEach-Object {
            $relPath = "valueSets/$($_.Name)"
            $bytes = [System.IO.File]::ReadAllBytes($_.FullName)
            $parts += @{
                path        = $relPath
                payload     = [Convert]::ToBase64String($bytes)
                payloadType = "InlineBase64"
            }
            Write-DebugLog "Added value set part: $relPath"
        }
    }

    return @{
        format = "VariableLibraryV1"
        parts  = $parts
    }
}

function Get-ExistingVariableLibrary {
    param(
        [string]$WorkspaceId,
        [string]$DisplayName,
        [hashtable]$Headers
    )

    $uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/variableLibraries"
    $response = Invoke-RestMethod -Uri $uri -Headers $Headers -Method GET -ErrorAction Stop

    $items = @()
    if ($response.value) {
        $items = $response.value
    }
    elseif ($response -is [array]) {
        $items = $response
    }

    $match = $items | Where-Object {
        $_.displayName -and $_.displayName.Equals($DisplayName, [System.StringComparison]::OrdinalIgnoreCase)
    } | Select-Object -First 1

    return $match
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

try {
    Write-Section "Invoke-VariableLibraryManagement – Action: $Action | DisplayName: $DisplayName"

    $headers = Get-FabricHeaders
    $baseUri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/variableLibraries"

    # Resolve folderId early (used on create)
    $folderId = Resolve-FolderId -FolderHierarchyJson $FolderHierarchy -RelativeFolderPath $FolderPath
    if ($folderId) {
        Write-DebugLog "Target folderId: $folderId"
    }

    # Build or parse definition
    $definition = $null
    if (-not [string]::IsNullOrWhiteSpace($DefinitionJson)) {
        Write-DebugLog "Using supplied DefinitionJson"
        $definition = $DefinitionJson | ConvertFrom-Json
    }
    elseif (-not [string]::IsNullOrWhiteSpace($PlatformFile)) {
        Write-DebugLog "Building definition from folder: $PlatformFile"
        $definition = Build-DefinitionFromFolder -PlatformFilePath $PlatformFile
    }

    switch ($Action) {

        'Get' {
            $existing = Get-ExistingVariableLibrary -WorkspaceId $WorkspaceId -DisplayName $DisplayName -Headers $headers
            if ($null -eq $existing) {
                Write-WarningLog "Variable Library '$DisplayName' not found."
                return $null
            }
            Write-Host ($existing | ConvertTo-Json -Depth 10)
            return $existing
        }

        'Delete' {
            $existing = Get-ExistingVariableLibrary -WorkspaceId $WorkspaceId -DisplayName $DisplayName -Headers $headers
            if ($null -eq $existing) {
                Write-WarningLog "Variable Library '$DisplayName' does not exist – nothing to delete."
                return $null
            }

            $deleteUri = "$baseUri/$($existing.id)"
            Write-DebugLog "Deleting Variable Library $($existing.id)"
            $null = Invoke-FabricRest -Method DELETE -Uri $deleteUri -Headers $headers
            Write-Section "Variable Library '$DisplayName' deleted successfully."
            return $null
        }

        'CreateOrUpdate' {
            if ($null -eq $definition) {
                throw "Either -DefinitionJson or -PlatformFile must be supplied for CreateOrUpdate."
            }

            $existing = Get-ExistingVariableLibrary -WorkspaceId $WorkspaceId -DisplayName $DisplayName -Headers $headers

            if ($null -ne $existing) {
                # ---------- UPDATE ----------
                Write-Section "Updating existing Variable Library '$DisplayName' (id=$($existing.id))"

                $updateUri = "$baseUri/$($existing.id)/updateDefinition"
                if ($UpdateMetadata) {
                    $updateUri += "?updateMetadata=true"
                }

                $body = @{
                    definition = $definition
                }

                $response = Invoke-FabricRest -Method POST -Uri $updateUri -Headers $headers -Body $body

                # Handle LRO
                if ($response.StatusCode -eq 202) {
                    $null = Wait-FabricLro -Response $response -Headers $headers
                }

                # Optionally update description via PATCH
                if (-not [string]::IsNullOrWhiteSpace($Description)) {
                    $patchBody = @{
                        description = $Description
                        displayName = $DisplayName
                    }
                    $patchUri = "$baseUri/$($existing.id)"
                    try {
                        $null = Invoke-FabricRest -Method PATCH -Uri $patchUri -Headers $headers -Body $patchBody
                    }
                    catch {
                        Write-WarningLog "PATCH for metadata failed (non-fatal): $($_.Exception.Message)"
                    }
                }

                # Re-fetch the item so caller gets the latest object
                $updated = Get-ExistingVariableLibrary -WorkspaceId $WorkspaceId -DisplayName $DisplayName -Headers $headers
                Write-Section "Variable Library '$DisplayName' updated successfully."
                Write-Host ($updated | ConvertTo-Json -Depth 8 -Compress)
                return $updated
            }
            else {
                # ---------- CREATE ----------
                Write-Section "Creating new Variable Library '$DisplayName'"

                $createBody = @{
                    displayName = $DisplayName
                    definition  = $definition
                }

                if (-not [string]::IsNullOrWhiteSpace($Description)) {
                    $createBody.description = $Description
                }
                if ($folderId) {
                    $createBody.folderId = $folderId
                }

                $response = Invoke-FabricRest -Method POST -Uri $baseUri -Headers $headers -Body $createBody

                $created = $null
                if ($response.StatusCode -eq 201 -or $response.StatusCode -eq 200) {
                    $created = $response.Content | ConvertFrom-Json
                }
                elseif ($response.StatusCode -eq 202) {
                    $null = Wait-FabricLro -Response $response -Headers $headers
                    Start-Sleep -Seconds 2
                    $created = Get-ExistingVariableLibrary -WorkspaceId $WorkspaceId -DisplayName $DisplayName -Headers $headers
                }

                if ($null -eq $created) {
                    $created = Get-ExistingVariableLibrary -WorkspaceId $WorkspaceId -DisplayName $DisplayName -Headers $headers
                }

                if ($null -eq $created) {
                    throw "Variable Library was created but could not be retrieved afterwards."
                }

                Write-Section "Variable Library '$DisplayName' created successfully (id=$($created.id))."
                Write-Host ($created | ConvertTo-Json -Depth 8 -Compress)
                return $created
            }
        }
    }
}
catch {
    Write-ErrorLog "Invoke-VariableLibraryManagement failed: $($_.Exception.Message)"
    Write-ErrorLog $_.ScriptStackTrace
    exit 1
}
