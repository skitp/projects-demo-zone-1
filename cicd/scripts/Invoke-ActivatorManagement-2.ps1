<#
.SYNOPSIS
    Deploys Microsoft Fabric Activator (Reflex) items from source folders.
.DESCRIPTION
    Scans for *.Reflex folders containing .platform and ReflexEntities.json,
    then creates or updates the corresponding Activator items in the specified Fabric workspace.
    Supports:
    - Folder hierarchy (creates folders as needed and places the item in the correct folder)
    - Description from .platform metadata
    - Environment-specific ID / token substitution in ReflexEntities.json ({{TOKEN}} or zero-GUID placeholders)
.PARAMETER WorkspaceId
    The ID of the Fabric workspace.
.PARAMETER SourcePath
    Path to the source directory containing *.Reflex folders.
.PARAMETER FabricToken
    Bearer token for Fabric API. Defaults to $env:FABRIC_TOKEN.
.PARAMETER FolderHierarchy
    Optional root folder hierarchy name/path (from $env:FOLDER_HIERARCHY).
.EXAMPLE
    .\Invoke-ActivatorManagement.ps1 -WorkspaceId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" -SourcePath "src/fabric"
#>
[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $false)]
    [string]$SourcePath,

    [Parameter(Mandatory = $false)]
    [string]$FabricToken = $env:FABRIC_TOKEN,

    [Parameter(Mandatory = $false)]
    [string]$FolderHierarchy = $env:FOLDER_HIERARCHY
)

$ErrorActionPreference = 'Stop'

if (-not $SourcePath) {
    if ($env:SYSTEM_DEFAULTWORKINGDIRECTORY) {
        $SourcePath = Join-Path $env:SYSTEM_DEFAULTWORKINGDIRECTORY 'src/fabric'
    } else {
        $SourcePath = (Get-Location).Path
    }
}

if (-not $FabricToken) {
    Write-Error "FABRIC_TOKEN is not set. Provide -FabricToken or set environment variable."
    exit 1
}

$baseUrl = "https://api.fabric.microsoft.com/v1"
$headers = @{
    "Authorization" = "Bearer $FabricToken"
    "Content-Type"  = "application/json"
}

# Common zero-GUID used as placeholder in sample definitions
$zeroGuid = "00000000-0000-0000-0000-000000000000"

function Invoke-FabricLRO {
    param (
        [string]$OperationId,
        [int]$MaxRetries = 60,
        [int]$RetryIntervalSec = 5
    )

    $statusUrl = "$baseUrl/operations/$OperationId"
    $retries = 0

    do {
        Start-Sleep -Seconds $RetryIntervalSec
        try {
            $opResponse = Invoke-RestMethod -Uri $statusUrl -Headers $headers -Method Get -ErrorAction Stop
            Write-Host "##[debug]Operation $OperationId status: $($opResponse.status) ($($opResponse.percentComplete)%)"
            if ($opResponse.status -in @("Succeeded", "Completed", "Failed")) {
                return $opResponse
            }
        }
        catch {
            Write-Warning "Error polling operation: $_"
        }
        $retries++
    } while ($retries -lt $MaxRetries)

    throw "Operation $OperationId did not complete within timeout."
}

function Get-ExistingReflexes {
    param ([string]$WsId)
    $url = "$baseUrl/workspaces/$WsId/reflexes?recursive=true"
    $all = @()
    $continuationToken = $null

    do {
        $requestUrl = $url
        if ($continuationToken) {
            $requestUrl = "$url&continuationToken=$([uri]::EscapeDataString($continuationToken))"
        }
        $response = Invoke-RestMethod -Uri $requestUrl -Headers $headers -Method Get
        if ($response.value) {
            $all += $response.value
        }
        $continuationToken = $response.continuationToken
    } while ($continuationToken)

    return $all
}

function Get-ExistingFolders {
    param ([string]$WsId)
    $url = "$baseUrl/workspaces/$WsId/folders?recursive=true"
    $all = @()
    $continuationToken = $null

    do {
        $requestUrl = $url
        if ($continuationToken) {
            $requestUrl = "$url&continuationToken=$([uri]::EscapeDataString($continuationToken))"
        }
        try {
            $response = Invoke-RestMethod -Uri $requestUrl -Headers $headers -Method Get
            if ($response.value) {
                $all += $response.value
            }
            $continuationToken = $response.continuationToken
        }
        catch {
            Write-Warning "Could not list folders (may not be supported or empty): $_"
            break
        }
    } while ($continuationToken)

    return $all
}

function Ensure-FolderPath {
    param (
        [string]$WsId,
        [string]$RelativePath,
        [array]$ExistingFolders
    )

    if ([string]::IsNullOrWhiteSpace($RelativePath) -or $RelativePath -eq "." -or $RelativePath -eq "/") {
        return $null
    }

    $segments = $RelativePath -split '[/\\]' | Where-Object { $_ -and $_ -notmatch '\.Reflex$' }
    if ($segments.Count -eq 0) { return $null }

    $parentId = $null
    $currentPath = ""

    foreach ($segment in $segments) {
        $currentPath = if ($currentPath) { "$currentPath/$segment" } else { $segment }

        $match = $ExistingFolders | Where-Object {
            $_.displayName -eq $segment -and (
                ($null -eq $parentId -and [string]::IsNullOrEmpty($_.parentFolderId)) -or
                $_.parentFolderId -eq $parentId
            )
        } | Select-Object -First 1

        if (-not $match) {
            Write-Host "##[debug]Creating folder: $segment (parent: $parentId)"
            $createBody = @{ displayName = $segment }
            if ($parentId) { $createBody.parentFolderId = $parentId }
            $bodyJson = $createBody | ConvertTo-Json -Compress

            $createUrl = "$baseUrl/workspaces/$WsId/folders"
            $resp = Invoke-RestMethod -Uri $createUrl -Headers $headers -Method Post -Body $bodyJson
            $match = $resp
            $ExistingFolders += $match
        }

        $parentId = $match.id
    }

    return $parentId
}

function Invoke-TokenSubstitution {
    param (
        [string]$Content,
        [string]$WorkspaceId
    )

    # Replace {{TOKEN}} with $env:TOKEN if set
    $Content = [regex]::Replace($Content, '\{\{([A-Za-z0-9_]+)\}\}', {
        param($m)
        $token = $m.Groups[1].Value
        $envVal = [Environment]::GetEnvironmentVariable($token)
        if ($envVal) { return $envVal }
        return $m.Value
    })

    # Replace zero-GUID placeholders with actual WorkspaceId where it makes sense
    # (common in workspaceId fields)
    if ($WorkspaceId -and $Content -match $zeroGuid) {
        $Content = $Content -replace [regex]::Escape($zeroGuid), $WorkspaceId
        Write-Host "##[debug]Replaced zero-GUID placeholders with WorkspaceId"
    }

    # Additional common env substitutions
    $replacements = @{
        "WORKSPACE_ID" = $WorkspaceId
        "TARGET_WORKSPACE_ID" = $WorkspaceId
    }
    foreach ($key in $replacements.Keys) {
        if ($replacements[$key]) {
            $Content = $Content -replace "\{\{$key\}\}", $replacements[$key]
        }
    }

    return $Content
}

function Deploy-Activator {
    param (
        [string]$DisplayName,
        [string]$Description,
        [string]$PlatformFile,
        [string]$EntitiesFile,
        [string]$WsId,
        [array]$ExistingItems,
        [string]$FolderId
    )

    Write-Host "##[section]Processing Activator: $DisplayName"

    if (-not (Test-Path $PlatformFile) -or -not (Test-Path $EntitiesFile)) {
        Write-Warning "Missing .platform or ReflexEntities.json for $DisplayName. Skipping."
        return
    }

    # Read and substitute tokens in entities
    $entitiesRaw = Get-Content $EntitiesFile -Raw -Encoding UTF8
    $entitiesRaw = Invoke-TokenSubstitution -Content $entitiesRaw -WorkspaceId $WsId
    $entitiesBytes = [System.Text.Encoding]::UTF8.GetBytes($entitiesRaw)

    # Platform file (usually no substitution needed, but support it)
    $platformRaw = Get-Content $PlatformFile -Raw -Encoding UTF8
    $platformRaw = Invoke-TokenSubstitution -Content $platformRaw -WorkspaceId $WsId
    $platformBytes = [System.Text.Encoding]::UTF8.GetBytes($platformRaw)

    $parts = @(
        @{
            path        = "ReflexEntities.json"
            payload     = [Convert]::ToBase64String($entitiesBytes)
            payloadType = "InlineBase64"
        },
        @{
            path        = ".platform"
            payload     = [Convert]::ToBase64String($platformBytes)
            payloadType = "InlineBase64"
        }
    )

    $definition = @{
        format = "json"
        parts  = $parts
    }

    $existing = $ExistingItems | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1

    if ($existing) {
        Write-Host "##[debug]Updating existing Activator: $($existing.id)"
        $updateUrl = "$baseUrl/workspaces/$WsId/reflexes/$($existing.id)/updateDefinition?updateMetadata=true"
        $body = @{ definition = $definition } | ConvertTo-Json -Depth 10 -Compress

        $response = Invoke-WebRequest -Uri $updateUrl -Headers $headers -Method Post -Body $body -UseBasicParsing
        if ($response.StatusCode -eq 202) {
            $opId = $response.Headers["x-ms-operation-id"]
            if (-not $opId) { $opId = ($response.Headers["Location"] -split '/')[-1] }
            $result = Invoke-FabricLRO -OperationId $opId
            if ($result.status -eq "Failed") {
                throw "Update failed for $DisplayName : $($result.error | ConvertTo-Json)"
            }
            Write-Host "##[debug]Update succeeded for $DisplayName"
        }
        elseif ($response.StatusCode -in 200, 201) {
            Write-Host "##[debug]Update completed synchronously for $DisplayName"
        }
        else {
            throw "Unexpected status $($response.StatusCode) for update of $DisplayName"
        }
    }
    else {
        Write-Host "##[debug]Creating new Activator: $DisplayName (folderId: $FolderId)"
        $createUrl = "$baseUrl/workspaces/$WsId/reflexes"
        $bodyObj = @{
            displayName = $DisplayName
            definition  = $definition
        }
        if ($Description) { $bodyObj.description = $Description }
        if ($FolderId) { $bodyObj.folderId = $FolderId }

        $body = $bodyObj | ConvertTo-Json -Depth 10 -Compress

        $response = Invoke-WebRequest -Uri $createUrl -Headers $headers -Method Post -Body $body -UseBasicParsing
        if ($response.StatusCode -eq 202) {
            $opId = $response.Headers["x-ms-operation-id"]
            if (-not $opId) { $opId = ($response.Headers["Location"] -split '/')[-1] }
            $result = Invoke-FabricLRO -OperationId $opId
            if ($result.status -eq "Failed") {
                throw "Create failed for $DisplayName : $($result.error | ConvertTo-Json)"
            }
            Write-Host "##[debug]Create succeeded for $DisplayName"
        }
        elseif ($response.StatusCode -in 200, 201) {
            Write-Host "##[debug]Create completed synchronously for $DisplayName"
        }
        else {
            $errorContent = $response.Content
            throw "Unexpected status $($response.StatusCode) for create of $DisplayName : $errorContent"
        }
    }
}

try {
    Write-Host "##[section]Deploying Fabric Activators (Reflex items)"

    if (-not (Test-Path $SourcePath)) {
        Write-Warning "Source path not found: $SourcePath"
        return
    }

    $reflexDirs = Get-ChildItem -Path $SourcePath -Recurse -Directory | Where-Object { $_.Name -like "*.Reflex" }

    if ($reflexDirs.Count -eq 0) {
        Write-Host "##[warning]No Activator (*.Reflex) folders found under $SourcePath"
        return
    }

    Write-Host "##[debug]Found $($reflexDirs.Count) Activator folder(s)"

    $existingReflexes = Get-ExistingReflexes -WsId $WorkspaceId
    Write-Host "##[debug]Found $($existingReflexes.Count) existing Activator(s) in workspace"

    $existingFolders = Get-ExistingFolders -WsId $WorkspaceId
    Write-Host "##[debug]Found $($existingFolders.Count) existing folder(s) in workspace"

    foreach ($dir in $reflexDirs) {
        $platformFile = Join-Path $dir.FullName ".platform"
        $entitiesFile = Join-Path $dir.FullName "ReflexEntities.json"

        if (-not (Test-Path $platformFile)) {
            Write-Warning "No .platform in $($dir.FullName). Skipping."
            continue
        }

        $platformJson = Get-Content $platformFile -Raw | ConvertFrom-Json
        $displayName = $platformJson.metadata.displayName
        if (-not $displayName) {
            $displayName = $dir.Name -replace '\.Reflex$', ''
        }

        $description = $null
        if ($platformJson.metadata.description) {
            $description = $platformJson.metadata.description
        }
        elseif ($platformJson.description) {
            $description = $platformJson.description
        }

        # Compute relative folder path for hierarchy
        $relative = $dir.FullName.Substring($SourcePath.Length).TrimStart('\','/')
        $folderPath = Split-Path $relative -Parent
        if ($FolderHierarchy) {
            $folderPath = if ($folderPath) { "$FolderHierarchy/$folderPath" } else { $FolderHierarchy }
        }

        $folderId = Ensure-FolderPath -WsId $WorkspaceId -RelativePath $folderPath -ExistingFolders $existingFolders

        Deploy-Activator -DisplayName $displayName `
                         -Description $description `
                         -PlatformFile $platformFile `
                         -EntitiesFile $entitiesFile `
                         -WsId $WorkspaceId `
                         -ExistingItems $existingReflexes `
                         -FolderId $folderId
    }

    Write-Host "##[section]Activator deployment completed"
}
catch {
    Write-Error "Error deploying Activators: $($_.Exception.Message)"
    Write-Error $_.ScriptStackTrace
    exit 1
}
