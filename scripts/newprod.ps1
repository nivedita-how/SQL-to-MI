<#
newprod.ps1 (auto-backup enabled)

Purpose  : Automate SQL Server -> Azure SQL Managed Instance (MI) migrations via Azure Database Migration Service (DMS)
           using Azure Blob Storage. Supports:
           - OFFLINE  : Take FULL to Blob automatically, then start DMS offline restore.
           - ONLINE   : Take seed FULL to Blob automatically, start DMS online (log shipping), optional log job + cutover.

Notes    : - DMS ONLINE to SQL MI requires an initial FULL backup (seed). Starting only with LOG backups is not supported.
             If you need no-seed streaming, consider Managed Instance link.  [docs]
           - Backup layout (ONLINE): put FULL/DIFF/LOG in same container, no appended backups, single log file per DB.

Author   : Packaged for Nivedita Sarang Dighe
Date     : 2026-02-04 (updated: parameterized infra inputs)
#>

[CmdletBinding()]
param(
    # ----------------- Azure context -----------------
    [Parameter(Mandatory=$false, HelpMessage='Azure Subscription Id (GUID)')]
    [ValidatePattern('^[0-9a-fA-F-]{36}$')]
    [string]$SubscriptionId,

    # ----------------- Mode selection ----------------
    [Parameter(Mandatory=$false)]
    [ValidateSet('Online','Offline')]
    [string]$Mode = 'Online',

    # ----------------- Source SQL --------------------
    [Parameter(Mandatory=$true)]
    [string]$SourceServer,

    [Parameter(Mandatory=$false)]
    [string]$SourceDbName = 'newdb',

    [Parameter(Mandatory=$true)]
    [string]$SourceUser,

    [Parameter(Mandatory=$true)]
    [securestring]$SourcePassword,

    # ----------------- Auto-backup options -----------
    [Parameter(Mandatory=$false, HelpMessage='When set, script will create Blob credential on source (if needed) and take FULL backup automatically.')]
    [switch]$AutoBackup,

    [Parameter(Mandatory=$false, HelpMessage='Generate a SAS for the container automatically (if BlobSasToken not provided). Default 24h.')]
    [int]$SasExpiryHours = 24,

    [Parameter(Mandatory=$false, HelpMessage='Optional SAS token (starting with ?, from portal or generated externally). If omitted and -AutoBackup is set, a SAS is generated.')]
    [string]$BlobSasToken,

    # OFFLINE-only: last backup name to pass to DMS (auto-filled when AutoBackup is used)
    [Parameter(Mandatory=$false, HelpMessage='(OFFLINE) EXACT blob file name of LAST backup (e.g., newdb_FULL_yyyymmddhhmm.bak or last .trn). If -AutoBackup is set, script will set this.')]
    [string]$OfflineLastBackupName,

    # ONLINE options
    [Parameter(Mandatory=$false, HelpMessage='For ONLINE: create a SQL Agent job that keeps LOG backups shipping to Blob.')]
    [switch]$CreateLogBackupJob,

    [Parameter(Mandatory=$false, HelpMessage='For ONLINE: perform cutover interactively when ready.')]
    [switch]$DoCutover,

    # ----------------- Dynamic Infra Inputs ----------
    [Parameter(Mandatory=$false, HelpMessage='Resource Group containing the SQL MI and migration service (default from previous script).')]
    [ValidateNotNullOrEmpty()]
    [string]$ResourceGroup = 'nivedita_resource',

    [Parameter(Mandatory=$false, HelpMessage='Azure SQL Managed Instance name (default from previous script).')]
    [ValidateNotNullOrEmpty()]
    [string]$ManagedInstanceName = 'free-sql-mi-0456877',

    [Parameter(Mandatory=$false, HelpMessage='Resource Group of the Storage Account (default from previous script).')]
    [ValidateNotNullOrEmpty()]
    [string]$StorageAccountRG = 'nivedita_resource',

    [Parameter(Mandatory=$false, HelpMessage='Storage Account name (default from previous script).')]
    [ValidateNotNullOrEmpty()]
    [string]$StorageAccountName = 'storn2',

    [Parameter(Mandatory=$false, HelpMessage='Blob container name (default from previous script).')]
    [ValidateNotNullOrEmpty()]
    [string]$ContainerName = 'st2cont',

    [Parameter(Mandatory=$false, HelpMessage='(Optional) Existing SQL Migration Service name. If not provided, a name is derived from the MI.')]
    [string]$SqlMigrationServiceName
)

# ----------------------------
# Helpers
# ----------------------------
function Ensure-Modules {
    param([string[]]$Names)
    foreach($m in $Names){
        if(-not (Get-Module -ListAvailable -Name $m)){
            Write-Host "Installing PowerShell module: $m" -ForegroundColor Yellow
            Install-Module $m -Scope CurrentUser -Force -ErrorAction Stop
        }
        Import-Module $m -ErrorAction Stop | Out-Null
    }
}

function Throw-If($Condition, [string]$Message){ if($Condition){ throw $Message } }

function Invoke-Tsql {
    param(
        [string]$Server, [string]$User, [securestring]$Password,
        [string]$Query, [int]$CommandTimeoutSec = 0
    )
    # Try Invoke-Sqlcmd if available; fallback to ADO.NET
    if (Get-Command Invoke-Sqlcmd -ErrorAction SilentlyContinue) {
        $plain = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password)
        )
        try {
            Invoke-Sqlcmd -ServerInstance $Server -Username $User -Password $plain -Encrypt Optional -TrustServerCertificate `
                          -Query $Query -QueryTimeout $CommandTimeoutSec | Out-Null
        } finally {
            if($plain){
                # Best effort cleanup
                [Runtime.InteropServices.Marshal]::ZeroFreeBSTR([Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password))
            }
        }
    } else {
        $plain = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password)
        )
        $cs = "Server=$Server;User ID=$User;Password=$plain;Encrypt=False;TrustServerCertificate=True;"
        $cn = New-Object System.Data.SqlClient.SqlConnection $cs
        $cn.Open()
        try {
            $cmd = $cn.CreateCommand()
            $cmd.CommandTimeout = $CommandTimeoutSec
            $cmd.CommandText = $Query
            $null = $cmd.ExecuteNonQuery()
        } finally {
            $cn.Close()
            if($plain){
                [Runtime.InteropServices.Marshal]::ZeroFreeBSTR([Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password))
            }
        }
    }
}

function Ensure-SourceBlobCredential {
    param(
        [string]$Server, [string]$User, [securestring]$Password,
        [string]$ContainerUrl, [string]$SasTokenNoQuestion
    )
    $credName = $ContainerUrl
    $tsql = @"
IF NOT EXISTS (SELECT 1 FROM sys.credentials WHERE name = N'$credName')
    CREATE CREDENTIAL [$credName] WITH IDENTITY = 'Shared Access Signature', SECRET = '$SasTokenNoQuestion';
ELSE
    ALTER CREDENTIAL [$credName] WITH IDENTITY = 'Shared Access Signature', SECRET = '$SasTokenNoQuestion';
"@
    Invoke-Tsql -Server $Server -User $User -Password $Password -Query $tsql
}

function New-FullBackupToBlob {
    param(
        [string]$Server, [string]$User, [securestring]$Password,
        [string]$DbName, [string]$ContainerUrl
    )
    $stamp = (Get-Date).ToString('yyyyMMdd_HHmmss')
    $file  = "{0}_FULL_{1}.bak" -f $DbName,$stamp
    $url   = "$ContainerUrl/$file"
    $tsql  = @"
BACKUP DATABASE [$DbName]
TO URL = N'$url'
WITH COPY_ONLY, COMPRESSION, CHECKSUM, STATS=10;
"@
    Invoke-Tsql -Server $Server -User $User -Password $Password -Query $tsql -CommandTimeoutSec 0
    return $file
}

function New-LogBackupToBlob {
    param(
        [string]$Server, [string]$User, [securestring]$Password,
        [string]$DbName, [string]$ContainerUrl
    )
    $stamp = (Get-Date).ToString('yyyyMMdd_HHmmss')
    $file  = "{0}_LOG_{1}.trn" -f $DbName,$stamp
    $url   = "$ContainerUrl/$file"
    $tsql  = @"
BACKUP LOG [$DbName]
TO URL = N'$url'
WITH COMPRESSION, CHECKSUM, STATS=10;
"@
    Invoke-Tsql -Server $Server -User $User -Password $Password -Query $tsql -CommandTimeoutSec 0
    return $file
}

function New-LogBackupJob {
    param(
        [string]$Server, [string]$User, [securestring]$Password,
        [string]$DbName, [string]$ContainerUrl, [int]$Minutes = 5
    )
    $jobName = "LogBackup_$DbName`_ToBlob"
    $cmdTsql = @"
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'$jobName')
BEGIN
    EXEC msdb.dbo.sp_add_job @job_name=N'$jobName', @enabled=1;
    EXEC msdb.dbo.sp_add_jobstep 
        @job_name=N'$jobName',
        @step_name=N'BackupLog',
        @subsystem=N'TSQL',
        @command=N'BACKUP LOG [$DbName] TO URL = N''$ContainerUrl/${DbName}_LOG_$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).trn'' WITH COMPRESSION, CHECKSUM, STATS=10;',
        @retry_attempts=3, @retry_interval=1;
    EXEC msdb.dbo.sp_add_schedule @schedule_name=N'$jobName`_Schedule', @freq_type=4, @freq_interval=1, @freq_subday_type=4, @freq_subday_interval=$Minutes, @active_start_time=000000;
    EXEC msdb.dbo.sp_attach_schedule @job_name=N'$jobName', @schedule_name=N'$jobName`_Schedule';
    EXEC msdb.dbo.sp_add_jobserver @job_name=N'$jobName';
END
"@
    Invoke-Tsql -Server $Server -User $User -Password $Password -Query $cmdTsql
    return $jobName
}

# ----------------------------
# Begin
# ----------------------------
$ErrorActionPreference = 'Stop'
Ensure-Modules -Names @('Az.Accounts','Az.Resources','Az.Storage','Az.Sql','Az.DataMigration')

# Sign in & subscription
try { $ctx = Get-AzContext -ErrorAction Stop } catch { Connect-AzAccount | Out-Null }
if($SubscriptionId){ Select-AzSubscription -SubscriptionId $SubscriptionId | Out-Null }

# Derive migration service name if not provided
if (-not $SqlMigrationServiceName -or [string]::IsNullOrWhiteSpace($SqlMigrationServiceName)) {
    # Safe default derived from MI name
    $SqlMigrationServiceName = "sqlmig-svc-$ManagedInstanceName"
}

# MI & Migration service
$mi        = Get-AzSqlInstance -Name $ManagedInstanceName -ResourceGroupName $ResourceGroup
$miScopeId = $mi.Id
$location  = $mi.Location

$svc = Get-AzDataMigrationSqlService -ResourceGroupName $ResourceGroup -Name $SqlMigrationServiceName -ErrorAction SilentlyContinue
if(-not $svc){
    Write-Host "Creating SQL Migration Service: $SqlMigrationServiceName in $location" -ForegroundColor Cyan
    $svc = New-AzDataMigrationSqlService -ResourceGroupName $ResourceGroup -SqlMigrationServiceName $SqlMigrationServiceName -Location $location
}
$svcId = $svc.Id

# Storage info + context
$stg    = Get-AzStorageAccount -ResourceGroupName $StorageAccountRG -Name $StorageAccountName
$stgId  = $stg.Id
$stgKey = (Get-AzStorageAccountKey -ResourceGroupName $StorageAccountRG -Name $StorageAccountName)[0].Value
$stgCtx = New-AzStorageContext -StorageAccountName $StorageAccountName -StorageAccountKey $stgKey

# Ensure container exists (useful when generating SAS)
$container = Get-AzStorageContainer -Context $stgCtx -Name $ContainerName -ErrorAction SilentlyContinue
if(-not $container){
    $null = New-AzStorageContainer -Context $stgCtx -Name $ContainerName -PublicAccess Off
}
$containerUrl = "https://$StorageAccountName.blob.core.windows.net/$ContainerName"

Write-Host '--- Inputs ---' -ForegroundColor Cyan
Write-Host ("Mode: {0} | DB: {1} | MI: {2} | RG: {3} | Storage: {4}/{5} (RG: {6})" -f $Mode,$SourceDbName,$ManagedInstanceName,$ResourceGroup,$StorageAccountName,$ContainerName,$StorageAccountRG)

# ----------------------------
# Auto-backup (FULL seed + optional LOG job)
# ----------------------------
if($AutoBackup){
    # SAS handling for BACKUP TO URL credential on source
    if([string]::IsNullOrWhiteSpace($BlobSasToken)){
        # Generate SAS for container (read/add/create/write/list/delete)
        $BlobSasToken = New-AzStorageContainerSASToken -Context $stgCtx -Name $ContainerName -Permission racwdl -ExpiryTime (Get-Date).AddHours($SasExpiryHours)
    }
    # SQL credential expects SAS WITHOUT leading '?'
    $sasNoQ = $BlobSasToken.TrimStart('?')

    # Ensure/refresh credential on source
    Ensure-SourceBlobCredential -Server $SourceServer -User $SourceUser -Password $SourcePassword -ContainerUrl $containerUrl -SasTokenNoQuestion $sasNoQ

    # Take seed FULL backup for BOTH modes (required for ONLINE; also convenient for OFFLINE)
    $fullName = New-FullBackupToBlob -Server $SourceServer -User $SourceUser -Password $SourcePassword -DbName $SourceDbName -ContainerUrl $containerUrl
    Write-Host ("Seed FULL created: {0}" -f $fullName) -ForegroundColor Green

    if($Mode -eq 'Offline'){
        # For OFFLINE, this FULL is the last backup unless you later add logs manually
        $OfflineLastBackupName = $fullName
    } else {
        # ONLINE: optionally create a log shipping job (or take an initial log)
        if($CreateLogBackupJob){
            $job = New-LogBackupJob -Server $SourceServer -User $SourceUser -Password $SourcePassword -DbName $SourceDbName -ContainerUrl $containerUrl -Minutes 5
            Write-Host ("Created/ensured SQL Agent job for LOG backups: {0}" -f $job) -ForegroundColor Green
        } else {
            try {
                $logName = New-LogBackupToBlob -Server $SourceServer -User $SourceUser -Password $SourcePassword -DbName $SourceDbName -ContainerUrl $containerUrl
                Write-Host ("Initial LOG created: {0}" -f $logName) -ForegroundColor Green
            } catch { Write-Warning "LOG backup step failed (will not block DMS). Ensure you have a log backup process running for ONLINE." }
        }
    }
}

# ----------------------------
# Build DMS source connection
# ----------------------------
$srcConnParams = @{
    SourceSqlConnectionAuthentication     = 'SqlAuthentication'
    SourceSqlConnectionDataSource         = $SourceServer
    SourceSqlConnectionUserName           = $SourceUser
    SourceSqlConnectionPassword           = $SourcePassword
    SourceDatabaseName                    = $SourceDbName
}

# ----------------------------
# Start DMS migration (Offline or Online)
# ----------------------------
if($Mode -eq 'Offline'){
    # DMS offline to MI expects the LAST backup file name  (LastBackupName)
    Throw-If ([string]::IsNullOrWhiteSpace($OfflineLastBackupName)) "For OFFLINE mode, -OfflineLastBackupName is required (auto-set when -AutoBackup is used)."

    # Validate blob exists
    $blob = Get-AzStorageBlob -Context $stgCtx -Container $ContainerName -Blob $OfflineLastBackupName -ErrorAction SilentlyContinue
    Throw-If (-not $blob) "Blob '$OfflineLastBackupName' not found in '$ContainerName'."

    Write-Host 'Starting OFFLINE migration...' -ForegroundColor Green
    $mig = New-AzDataMigrationToSqlManagedInstance `
            -ResourceGroupName $ResourceGroup `
            -ManagedInstanceName $ManagedInstanceName `
            -TargetDbName $SourceDbName `
            -Kind 'SqlMI' `
            -Scope $miScopeId `
            -MigrationService $svcId `
            -AzureBlobStorageAccountResourceId $stgId `
            -AzureBlobAccountKey $stgKey `
            -AzureBlobContainerName $ContainerName `
            -Offline `
            -OfflineConfigurationLastBackupName $OfflineLastBackupName `
            @srcConnParams
} else {
    # ONLINE requires a seed FULL; we created it above when -AutoBackup was used.
    Write-Host 'Starting ONLINE migration (seed FULL + continuous LOG restores)...' -ForegroundColor Green
    $mig = New-AzDataMigrationToSqlManagedInstance `
            -ResourceGroupName $ResourceGroup `
            -ManagedInstanceName $ManagedInstanceName `
            -TargetDbName $SourceDbName `
            -Kind 'SqlMI' `
            -Scope $miScopeId `
            -MigrationService $svcId `
            -AzureBlobStorageAccountResourceId $stgId `
            -AzureBlobAccountKey $stgKey `
            -AzureBlobContainerName $ContainerName `
            @srcConnParams
}

# ----------------------------
# Poll migration status
# ----------------------------
Write-Host 'Polling migration status (Ctrl+C to stop polling)...' -ForegroundColor Cyan
$inProgressStates = @('InProgress','Accepted')
$ongoingStatuses  = @('InProgress','FullBackupUploadCompleted','FullBackupRestoreInProgress','LogShippingInProgress')

while($true){
    Start-Sleep -Seconds 20
    $state = Get-AzDataMigrationToSqlManagedInstance -ResourceGroupName $ResourceGroup -ManagedInstanceName $ManagedInstanceName -TargetDbName $SourceDbName -ErrorAction SilentlyContinue
    if($null -eq $state){
        Write-Warning "Migration resource not found yet. If the last call failed, check errors above."
        continue
    }
    Write-Host (" ProvisioningState = {0} | MigrationStatus = {1}" -f $state.ProvisioningState,$state.MigrationStatus)

    if(($Mode -eq 'Offline') -and ($state.ProvisioningState -notin $inProgressStates)){ break }
    if(($Mode -eq 'Online')  -and ($state.MigrationStatus   -notin $ongoingStatuses)){ break }
}

if($Mode -eq 'Offline'){
    Write-Host 'OFFLINE migration finished. Validate on MI and switch application connections.' -ForegroundColor Yellow
    return
}

# ----------------------------
# ONLINE: optional cutover
# ----------------------------
if($DoCutover){
    Write-Host 'Ready to perform CUTOVER for ONLINE migration.' -ForegroundColor Yellow
    Read-Host 'Press ENTER to initiate cutover (or Ctrl+C to abort)'
    $mig = Get-AzDataMigrationToSqlManagedInstance -ResourceGroupName $ResourceGroup -ManagedInstanceName $ManagedInstanceName -TargetDbName $SourceDbName
    Invoke-AzDataMigrationCutoverToSqlManagedInstance `
        -ResourceGroupName $ResourceGroup `
        -ManagedInstanceName $ManagedInstanceName `
        -TargetDbName $SourceDbName `
        -MigrationOperationId $mig.MigrationOperationId | Out-Null
    Write-Host 'Cutover invoked. Monitor until Completed.' -ForegroundColor Green
} else {
    Write-Host 'ONLINE migration is running. Keep LOG backups going (job or external process) until you decide to cut over.' -ForegroundColor Yellow
}
