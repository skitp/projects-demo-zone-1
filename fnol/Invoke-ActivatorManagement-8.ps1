<#
.SYNOPSIS
    Deploys Microsoft Fabric Activator (Reflex) items from source folders.
.DESCRIPTION
    Scans for *.Reflex folders (Git-managed by Fabric integration) and deploys them.
    Tokenized definition templates are loaded from DevOpsServices/pipelines/templates/
    (outside the Git-managed folders) and substituted in memory only.
    Supports:
    - Folder hierarchy
    - Description from .platform
    - Environment-specific token substitution ({{TOKEN}})
.PARAMETER WorkspaceId
    The ID of the Fabric workspace.
.PARAMETER SourcePath
    Path to the source directory containing *.Reflex folders.
.PARAMETER TemplatesPath
    Path to tokenized templates. Defaults to DevOpsServices/pipelines/templates.
.PARAMETER FabricToken
    Bearer token for Fabric API. Defaults to $env:FABRIC_TOKEN.
.PARAMETER FolderHierarchy
    Optional root folder hierarchy name/path.
.EXAMPLE
    .\Invoke-ActivatorManagement.ps1 -WorkspaceId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
#>
[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $false)]
    [string]$SourcePath,

    [Parameter(Mandatory = $false)]
    [string]$TemplatesPath,

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

if (-not $TemplatesPath) {
    if ($env:SYSTEM_DEFAULTWORKINGDIRECTORY) {
        $TemplatesPath = Join-Path $env:SYSTEM_DEFAULTWORKINGDIRECTORY 'DevOpsServices/pipelines/templates'
    } else {
        $TemplatesPath = Join-Path (Get-Location).Path 'DevOpsServices/pipelines/templates'
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
            Write-Warning "Could not list folders: $_"
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

    foreach ($segment in $segments) {
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
            try {
                $resp = Invoke-RestMethod -Uri $createUrl -Headers $headers -Method Post -Body $bodyJson -ErrorAction Stop
            }
            catch {
                $errBody = $_.ErrorDetails.Message
                if (-not $errBody -and $_.Exception.Response) {
                    $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                    $errBody = $reader.ReadToEnd()
                }
                throw "Failed to create folder '$segment': $($_.Exception.Message) | Response: $errBody"
            }
            $match = $resp
            $ExistingFolders += $match
        }

        $parentId = $match.id
    }

    return $parentId
}

function Invoke-FabricRequest {
    param (
        [string]$Uri,
        [string]$Method = "Post",
        [string]$Body
    )

    try {
        $params = @{
            Uri             = $Uri
            Headers         = $headers
            Method          = $Method
            UseBasicParsing = $true
            ErrorAction     = 'Stop'
        }
        if ($Body) { $params.Body = $Body }

        return Invoke-WebRequest @params
    }
    catch {
        $statusCode = $null
        $errBody = $_.ErrorDetails.Message

        if ($_.Exception.Response) {
            $statusCode = [int]$_.Exception.Response.StatusCode
            try {
                $stream = $_.Exception.Response.GetResponseStream()
                if ($stream) {
                    $reader = New-Object System.IO.StreamReader($stream)
                    $errBody = $reader.ReadToEnd()
                    $reader.Close()
                }
            }
            catch { }
        }

        $msg = "HTTP $statusCode calling $Uri : $($_.Exception.Message)"
        if ($errBody) { $msg += "`nResponse body: $errBody" }
        throw $msg
    }
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

    # Replace zero-GUID placeholders with actual WorkspaceId
    if ($WorkspaceId -and $Content -match $zeroGuid) {
        $Content = $Content -replace [regex]::Escape($zeroGuid), $WorkspaceId
        Write-Host "##[debug]Replaced zero-GUID placeholders with WorkspaceId"
    }

    return $Content
}

function Resolve-EntitiesFile {
    param (
        [string]$DisplayName,
        [string]$FolderName,
        [string]$OfficialEntitiesFile,
        [string]$TemplatesPath
    )

    # Preferred: tokenized template outside Git-managed folder
    $candidates = @(
        (Join-Path $TemplatesPath "$DisplayName.ReflexEntities.json"),
        (Join-Path $TemplatesPath "$FolderName.ReflexEntities.json"),
        (Join-Path $TemplatesPath "$DisplayName.json"),
        (Join-Path $TemplatesPath "ReflexEntities.$DisplayName.json")
    )

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            Write-Host "##[debug]Using tokenized template: $candidate"
            return $candidate
        }
    }

    # Fallback: official file inside *.Reflex folder (Git-managed)
    if (Test-Path $OfficialEntitiesFile) {
        Write-Host "##[debug]No tokenized template found. Using official file: $OfficialEntitiesFile"
        return $OfficialEntitiesFile
    }

    return $null
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

    if (-not (Test-Path $PlatformFile) -or -not $EntitiesFile -or -not (Test-Path $EntitiesFile)) {
        Write-Warning "Missing .platform or ReflexEntities source for $DisplayName. Skipping."
        return
    }

    # Read and substitute tokens in memory only (never writes back to source)
    $entitiesRaw = Get-Content $EntitiesFile -Raw -Encoding UTF8
    $entitiesRaw = Invoke-TokenSubstitution -Content $entitiesRaw -WorkspaceId $WsId
    $entitiesBytes = [System.Text.Encoding]::UTF8.GetBytes($entitiesRaw)

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

    # Force parts to be a proper array and use the Default format expected by current Reflex API
    $partsArray = @($parts)
    $definition = @{
        format = "Default"
        parts  = $partsArray
    }

    $existing = $ExistingItems | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1

    if ($existing) {
        Write-Host "##[debug]Updating existing Activator: $($existing.id)"
        $updateUrl = "$baseUrl/workspaces/$WsId/reflexes/$($existing.id)/updateDefinition?updateMetadata=true"
        $body = @{ definition = $definition } | ConvertTo-Json -Depth 10 -Compress

        $response = Invoke-FabricRequest -Uri $updateUrl -Body $body
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
            $errorContent = $response.Content
            throw "Unexpected status $($response.StatusCode) for update of $DisplayName : $errorContent"
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

        # Sanitized debug (truncate long base64 payloads)
        $sanitized = $body -replace '"payload":"[^"]{100,}"', '"payload":"<truncated>"'
        Write-Host "##[debug]Create request body (sanitized): $sanitized"

        try {
            $response = Invoke-FabricRequest -Uri $createUrl -Body $body
        }
        catch {
            Write-Host "##[warning]Create with definition failed. Falling back to empty create + updateDefinition. Error: $($_.Exception.Message)"
            # Fallback: create empty item, then update definition
            $emptyBodyObj = @{ displayName = $DisplayName }
            if ($Description) { $emptyBodyObj.description = $Description }
            if ($FolderId) { $emptyBodyObj.folderId = $FolderId }
            $emptyBody = $emptyBodyObj | ConvertTo-Json -Compress

            $createResp = Invoke-FabricRequest -Uri $createUrl -Body $emptyBody
            $created = $createResp.Content | ConvertFrom-Json
            $newId = $created.id
            if (-not $newId) { throw "Empty create succeeded but no id returned: $($createResp.Content)" }

            Write-Host "##[debug]Empty Activator created with id $newId. Applying definition..."
            $updateUrl = "$baseUrl/workspaces/$WsId/reflexes/$newId/updateDefinition?updateMetadata=true"
            $updateBody = @{ definition = $definition } | ConvertTo-Json -Depth 10 -Compress
            $response = Invoke-FabricRequest -Uri $updateUrl -Body $updateBody
        }
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
    Write-Host "##[debug]SourcePath: $SourcePath"
    Write-Host "##[debug]TemplatesPath: $TemplatesPath"

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
        $officialEntitiesFile = Join-Path $dir.FullName "ReflexEntities.json"

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

        # Resolve entities source: prefer tokenized template outside Git folder
        $entitiesFile = Resolve-EntitiesFile -DisplayName $displayName `
                                              -FolderName $dir.Name `
                                              -OfficialEntitiesFile $officialEntitiesFile `
                                              -TemplatesPath $TemplatesPath

        if (-not $entitiesFile) {
            Write-Warning "No ReflexEntities source found for $displayName. Skipping."
            continue
        }

        # Compute relative folder path purely from filesystem (ignore FOLDER_HIERARCHY if it is JSON or otherwise invalid)
        $relative = $dir.FullName.Substring($SourcePath.Length).TrimStart('\','/')
        $folderPath = Split-Path $relative -Parent

        # Only prepend FolderHierarchy if it looks like a simple path (not JSON / object dump)
        if ($FolderHierarchy -and $FolderHierarchy -notmatch '^\s*[\[\{]' -and $FolderHierarchy -notmatch '"WorkspaceId"') {
            $folderPath = if ($folderPath) { "$FolderHierarchy/$folderPath" } else { $FolderHierarchy }
        }
        else {
            if ($FolderHierarchy) {
                Write-Host "##[warning]Ignoring FOLDER_HIERARCHY because it does not look like a simple path (value starts with JSON). Using filesystem relative path instead: '$folderPath'"
            }
        }

        Write-Host "##[debug]Computed folder path for '$displayName': '$folderPath'"

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
