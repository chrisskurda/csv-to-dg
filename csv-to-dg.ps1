# hr-sync.ps1

param (
    [string]$RollbackDate = "",
    [int]$UndoChangeId = 0,
    [switch]$ListRollbackDates,
    [string]$ShowChangesFor = ""
)

# Load Config
$configPath = "./config.json"
if (!(Test-Path $configPath)) {
    throw "Config file not found: $configPath"
}
$config = Get-Content $configPath -Raw | ConvertFrom-Json

function Ensure-DistributionGroup {
    $group = Get-ADGroup -Filter "Name -eq '$($config.dist_group.name)'" -ErrorAction SilentlyContinue
    if (-not $group) {
        try {
            New-ADGroup `
                -Name $config.dist_group.name `
                -GroupScope Universal `
                -GroupCategory Distribution `
                -Path $config.dist_group.ou_path `
                -Server $config.ad.domain_controller `
                -OtherAttributes @{ mail = $config.dist_group.email }

            Write-Log "Created distribution group: $($config.dist_group.name)"
            $group = Get-ADGroup -Filter "Name -eq '$($config.dist_group.name)'" -Server $config.ad.domain_controller
            Log-Change -action "add" -object_type "group" -target $group.DistinguishedName -attribute "mail" -old_value "" -new_value $config.dist_group.email
        } catch {
            Write-Log "Failed to create group: $_"
            throw
        }
    } else {
        Write-Log "Distribution group already exists: $($config.dist_group.name)"
    }

    $exchangeProps = @{}
    if ($config.exchange_settings.auth_orig) {
        $exchangeProps["authOrig"] = $config.exchange_settings.auth_orig
    }
    if ($config.exchange_settings.unauth_orig) {
        $exchangeProps["unauthOrig"] = $config.exchange_settings.unauth_orig
    }
    if ($config.exchange_settings.dl_mem_reject_perms) {
        $exchangeProps["dLMemRejectPerms"] = $config.exchange_settings.dl_mem_reject_perms
    }
    if ($config.exchange_settings.dl_mem_submit_perms) {
        $exchangeProps["dLMemSubmitPerms"] = $config.exchange_settings.dl_mem_submit_perms
    }
    if ($config.exchange_settings.require_auth -eq $true) {
        $exchangeProps["msExchRequireAuthToSendTo"] = $true
    }
    if ($config.exchange_settings.hide_from_gal -eq $true) {
        $exchangeProps["msExchHideFromAddressLists"] = $true
    }

    if ($exchangeProps.Count -gt 0) {
        Set-ADGroup -Identity $group.DistinguishedName -Replace $exchangeProps
        foreach ($key in $exchangeProps.Keys) {
            Log-Change -action "modify" -object_type "group" -target $group.DistinguishedName -attribute $key -old_value "" -new_value "$($exchangeProps[$key])"
        }
        Write-Log "Applied Exchange settings to group: $($config.dist_group.name)"
    }
}

function Send-EmailReport {
    param ([string]$body)
    if ($config.email.enabled) {
        if (-not $config.email.smtp_server) {
            throw "SMTP server not specified in config."
        }
        Send-MailMessage `
            -From $config.email.from `
            -To $config.email.to `
            -Subject $config.email.subject `
            -Body $body `
            -SmtpServer $config.email.smtp_server
    }
}

function Write-Log {
    param ([string]$message)
    $logFile = Join-Path $config.paths.log_dir "hr-sync-$(Get-Date -Format 'yyyy-MM-dd').log"
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$timestamp - $message" | Out-File $logFile -Append
    return "$timestamp - $message"
}

function Log-Change {
    param (
        [string]$action,
        [string]$object_type,
        [string]$target,
        [string]$attribute,
        [string]$old_value,
        [string]$new_value
    )
    if ($config.use_database) {
        $query = "INSERT INTO changes (timestamp, action, object_type, target, attribute, old_value, new_value) VALUES (@ts, @act, @obj, @tgt, @attr, @old, @new);"
        $params = @{
            ts   = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
            act  = $action
            obj  = $object_type
            tgt  = $target
            attr = $attribute
            old  = $old_value
            new  = $new_value
        }
        Invoke-SqliteQuery -DataSource $dbPath -Query $query -SqlParameters $params
    }
}

function List-RollbackDates {
    $results = Invoke-SqliteQuery -DataSource $dbPath -Query "SELECT DISTINCT date FROM imports ORDER BY date DESC;"
    Write-Host "Available rollback dates:" -ForegroundColor Cyan
    foreach ($row in $results) {
        Write-Host " - $($row.date)"
    }
}

function Show-ChangesForDate {
    param ([string]$dateStr)
    $results = Invoke-SqliteQuery -DataSource $dbPath -Query @"
        SELECT * FROM changes WHERE substr(timestamp, 1, 10) = @date ORDER BY timestamp;
"@ -SqlParameters @{ date = $dateStr }
    if (!$results) {
        Write-Host "No changes recorded for $dateStr." -ForegroundColor Yellow
        return
    }
    Write-Host "Changes for ${dateStr}:" -ForegroundColor Cyan
    foreach ($change in $results) {
        Write-Host "$($change.timestamp) [$($change.action)] $($change.object_type) $($change.target) : $($change.attribute) = '$($change.old_value)' -> '$($change.new_value)'"
    }
}

function Reduce-CSV {
    param (
        [string]$inputPath,
        [string]$outputPath
    )
    $csv = Import-Csv $inputPath
    $keep = $config.columns_to_keep
    $reduced = $csv | Select-Object -Property $keep
    $reduced | Export-Csv -NoTypeInformation -Path $outputPath
    return $reduced
}

function Get-RawCsvText {
    param ([System.Object[]]$csv)
    $stringWriter = New-Object System.IO.StringWriter
    $csv | ConvertTo-Csv -NoTypeInformation | ForEach-Object {
        $stringWriter.WriteLine($_)
    }
    return $stringWriter.ToString()
}

function Rollback {
    param ([string]$dateStr)
    $backup = Invoke-SqliteQuery -DataSource $dbPath -Query "SELECT raw_csv FROM imports WHERE date = @date ORDER BY id DESC LIMIT 1;" -SqlParameters @{ date = $dateStr }
    if (!$backup -or !$backup[0].raw_csv) {
        Write-Log "No backup data found for rollback date $dateStr"
        return
    }
    $raw = $backup[0].raw_csv
    $tempFile = Join-Path $env:TEMP "hr-restore-$dateStr.csv"
    $raw | Out-File -FilePath $tempFile -Encoding UTF8
    Write-Log "Rolling back using database snapshot from $dateStr"
    Update-GroupMembership $tempFile
    Remove-Item $tempFile -Force
}

# --- Main Execution ---
try {
    if ($ListRollbackDates) {
        List-RollbackDates
    } elseif ($ShowChangesFor) {
        Show-ChangesForDate -dateStr $ShowChangesFor
    } elseif ($UndoChangeId -gt 0) {
        Undo-Change -changeId $UndoChangeId
    } elseif ($RollbackDate) {
        Rollback $RollbackDate
    } else {
        $timestamp = Get-Date -Format "yyyy-MM-dd"
        $filteredCsvPath = Join-Path $config.paths.output_dir "filtered-HR-export-$timestamp.csv"

        $csvData = Reduce-CSV $config.paths.input_csv $filteredCsvPath
        $entryCount = $csvData.Count
        $rawCsvText = Get-RawCsvText -csv $csvData

        Ensure-DistributionGroup
        Update-GroupMembership $filteredCsvPath
        Cleanup-OldFiles

        $status = "success"
        $logSummary = "Processed $entryCount entries."
        Send-EmailReport "HR sync completed successfully for $timestamp."

        if ($config.use_database) {
            $insertQuery = "INSERT INTO imports (date, filename, entry_count, status, log, raw_csv) VALUES (@date, @file, @count, @status, @log, @raw);"
            $parameters = @{
                date    = $timestamp
                file    = $filteredCsvPath
                count   = $entryCount
                status  = $status
                log     = $logSummary
                raw     = $rawCsvText
            }
            Invoke-SqliteQuery -DataSource $dbPath -Query $insertQuery -SqlParameters $parameters
        }
    }
} catch {
    $err = Write-Log "Error: $_"
    Send-EmailReport "HR sync failed: $_"
    if ($config.use_database -and $dbPath) {
        $insertQuery = "INSERT INTO imports (date, filename, entry_count, status, log, raw_csv) VALUES (@date, @file, @count, @status, @log, @raw);"
        $parameters = @{
            date    = (Get-Date -Format "yyyy-MM-dd")
            file    = "ERROR"
            count   = 0
            status  = "failure"
            log     = $err
            raw     = ""
        }
        Invoke-SqliteQuery -DataSource $dbPath -Query $insertQuery -SqlParameters $parameters
    }
    throw
}
