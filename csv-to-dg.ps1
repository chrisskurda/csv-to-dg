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
$dbPath = $config.database.path

# Tracking Variables
$scriptStartTime = Get-Date
$failedLookups = @()
$membersAdded = @()
$membersRemoved = @()
$exchangeChanges = @()

function Ensure-DistributionGroup {
    $group = Get-ADGroup -Filter "Name -eq '$($config.dist_group.name)'" -Server $config.ad.domain_controller -Properties * -ErrorAction SilentlyContinue
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
            $group = Get-ADGroup -Filter "Name -eq '$($config.dist_group.name)'" -Server $config.ad.domain_controller -Properties *
        } catch {
            Write-Log "Failed to create group: $_"
            throw
        }
    } else {
        Write-Log "Distribution group already exists: $($config.dist_group.name)"
    }

    $exchangeProps = @{}
    foreach ($key in @("authOrig", "unauthOrig", "dLMemRejectPerms", "dLMemSubmitPerms", "msExchRequireAuthToSendTo", "msExchHideFromAddressLists")) {
        $desired = $null
        switch ($key) {
            "authOrig"                  { $desired = $config.exchange_settings.auth_orig }
            "unauthOrig"               { $desired = $config.exchange_settings.unauth_orig }
            "dLMemRejectPerms"         { $desired = $config.exchange_settings.dl_mem_reject_perms }
            "dLMemSubmitPerms"         { $desired = $config.exchange_settings.dl_mem_submit_perms }
            "msExchRequireAuthToSendTo" { $desired = $config.exchange_settings.require_auth }
            "msExchHideFromAddressLists" { $desired = $config.exchange_settings.hide_from_gal }
        }
        if ($null -ne $desired) {
            $current = $group.$key
            $currentVal = if ($current -is [array]) { $current } else { @($current) }
            $desiredVal = if ($desired -is [array]) { $desired } else { @($desired) }

            if (-not (@($currentVal) -join "|" -eq @($desiredVal) -join "|")) {
                $exchangeProps[$key] = $desired
                $exchangeChanges += " - ${key}: '$currentVal' -> '$desiredVal'"
            }
        }
    }

    if ($exchangeProps.Count -gt 0) {
        Set-ADGroup -Identity $group.DistinguishedName -Replace $exchangeProps
        Write-Log "Updated Exchange settings for group: $($config.dist_group.name)"
    } else {
        Write-Log "No Exchange settings needed update."
    }
}

function Write-Log {
    param ([string]$message)
    $logFile = Join-Path $config.paths.log_dir "hr-sync-$(Get-Date -Format 'yyyy-MM-dd').log"
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$timestamp - $message" | Out-File $logFile -Append
    return "$timestamp - $message"
}

function Send-EmailReport {
    param (
        [string]$body,
        [string[]]$attachments = @()
    )

    if ($config.email.enabled) {
        $params = @{
            From       = $config.email.from
            To         = $config.email.to
            Subject    = $config.email.subject
            Body       = $body
            SmtpServer = $config.email.smtp_server
        }

        if ($attachments.Count -gt 0) {
            $params["Attachments"] = $attachments
        }

        Send-MailMessage @params
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

function Cleanup-OldFiles {
    $retention = $config.retention_days
    Get-ChildItem -Path $config.paths.output_dir -Filter "filtered-HR-export-*.csv" | 
        Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$retention) } | 
        Remove-Item -Force
}

function Update-GroupMembership {
    param ([string]$csvPath)
    $csv = Import-Csv $csvPath
    $group = Get-ADGroup -Filter "Name -eq '$($config.dist_group.name)'" -Server $config.ad.domain_controller
    $existingMembers = Get-ADGroupMember -Identity $group.DistinguishedName | Select-Object -ExpandProperty DistinguishedName

    $targetMembers = $csv | ForEach-Object {
        $email = $_."E-Mail".Trim()
        if (![string]::IsNullOrWhiteSpace($email)) {
            $safeEmail = $email -replace "'", "''"
            try {
                $user = Get-ADUser -Filter "mail -eq '$safeEmail'" -Server $config.ad.domain_controller -ErrorAction Stop
                if ($user) { return $user.DistinguishedName }
            } catch {
                $logMessage = "Could not find AD user with email '$email'"
                $failedLookups += $logMessage
                Write-Log $logMessage | Out-Null
            }
        }
    } | Where-Object { $_ -ne $null } | Sort-Object -Unique

    $toAdd = $targetMembers | Where-Object { $_ -notin $existingMembers }
    $toRemove = $existingMembers | Where-Object { $_ -notin $targetMembers }

    foreach ($dn in $toAdd) {
        Add-ADGroupMember -Identity $group.DistinguishedName -Members $dn
        $membersAdded += $dn
        Write-Log "Added $dn to $($group.Name)"
    }
    foreach ($dn in $toRemove) {
        Remove-ADGroupMember -Identity $group.DistinguishedName -Members $dn -Confirm:$false
        $membersRemoved += $dn
        Write-Log "Removed $dn from $($group.Name)"
    }
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
    $path = Join-Path $config.paths.output_dir "filtered-HR-export-$dateStr.csv"
    if (!(Test-Path $path)) {
        Write-Log "Rollback file not found for $dateStr"
        return
    }
    Update-GroupMembership -csvPath $path
    Write-Log "Rollback completed for $dateStr"
}

# Main
try {
    if ($ListRollbackDates) {
        Get-ChildItem -Path $config.paths.output_dir -Filter "filtered-HR-export-*.csv" |
            ForEach-Object { $_.BaseName -replace "^filtered-HR-export-", "" } | Sort-Object -Descending | ForEach-Object { Write-Host $_ }
    } elseif ($ShowChangesFor) {
        Rollback -dateStr $ShowChangesFor
    } elseif ($RollbackDate) {
        Rollback -dateStr $RollbackDate
    } else {
        $timestamp = Get-Date -Format "yyyy-MM-dd"
        $filteredCsvPath = Join-Path $config.paths.output_dir "filtered-HR-export-$timestamp.csv"

        $csvData = Reduce-CSV $config.paths.input_csv $filteredCsvPath
        $entryCount = $csvData.Count
        $rawCsvText = Get-RawCsvText -csv $csvData

        Ensure-DistributionGroup
        Update-GroupMembership -csvPath $filteredCsvPath
        Cleanup-OldFiles

        $csvInfo = Get-Item $config.paths.input_csv
        $group = Get-ADGroup -Filter "Name -eq '$($config.dist_group.name)'" -Properties mail -Server $config.ad.domain_controller
        $finalMembers = Get-ADGroupMember -Identity $group.DistinguishedName
        $finalCount = $finalMembers.Count

        $reportBody = @()
        $reportBody += "HR Sync Report"
        $reportBody += "-------------------------"
        $reportBody += "Script Run Time:       $($scriptStartTime)"
        $reportBody += "Input CSV:             $($csvInfo.FullName)"
        $reportBody += "CSV Last Modified:     $($csvInfo.LastWriteTime)"
        $reportBody += "Entries in CSV:        $entryCount"
        $reportBody += ""
        $reportBody += "Group Name:            $($group.Name)"
        $reportBody += "Group Email:           $($group.mail)"
        $reportBody += ""
        if ($exchangeChanges.Count -gt 0) {
            $reportBody += "Exchange Settings Updated:"
            $reportBody += $exchangeChanges
            $reportBody += ""
        }
        $reportBody += "Members Added:         $($membersAdded.Count)"
        $reportBody += "Members Removed:       $($membersRemoved.Count)"
        $reportBody += "Group Member Count:    $finalCount"
        $reportBody += ""
        if ($failedLookups.Count -gt 0) {
            $reportBody += "Failed Lookups:"
            $reportBody += $failedLookups | ForEach-Object { " - $_" }
        }
        $reportBody += ""
        $reportBody = $reportBody -join "`n"

        $logPath = Join-Path $config.paths.log_dir "hr-sync-$timestamp.log"
        $attachments = @()
        if (Test-Path $logPath) { $attachments += $logPath }
        if (Test-Path $filteredCsvPath) { $attachments += $filteredCsvPath }

        Write-Log "HR sync completed successfully. $entryCount users processed."
        Send-EmailReport -body $reportBody -attachments $attachments
    }
} catch {
    $err = Write-Log "Error: $_"
    Send-EmailReport -body "HR sync failed: $_"
    throw
}
