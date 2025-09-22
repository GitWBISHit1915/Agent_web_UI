$path = "C:\Projects\Agent_web_UI"
$logFilePath = "C:\Projects\Agent_web_UI\log.txt"
$treeFilePath = "C:\Projects\Agent_web_UI\directory_tree.txt"

# Define exclusions
$exclusions = @(
    "C:\Projects\Agent_web_UI\.git",
    "C:\Projects\SQL\wbis_chat\.venv",
    "C:\Projects\SQL\wbis_chat\.vs",
    "C:\Projects\SQL\wbis_chat\__pycache__"
    "*.tmp",  # Example of a file type to exclude
    "*.log"   # Exclude other log files
)

# Function to check exclusions
function IsExcluded ($path) {
    foreach ($exclusion in $exclusions) {
        if ($path -like $exclusion) {
            return $true
        }
    }
    return $false
}

# Function to create directory tree
function Generate-DirectoryTree {
    $output = @()
    function Get-DirectoryContents($currentPath) {
        Get-ChildItem -Path $currentPath -Directory | ForEach-Object {
            if (-not (IsExcluded $_.FullName)) {
                $output += $_.FullName
                Get-DirectoryContents $_.FullName
            }
        }
        Get-ChildItem -Path $currentPath | ForEach-Object {
            if (-not (IsExcluded $_.FullName)) {
                $output += $_.FullName
            }
        }
    }

    Get-DirectoryContents $path

    # Write the output to the directory_tree.txt file
    $output | Out-File -FilePath $treeFilePath
}

# Initial directory tree generation
Generate-DirectoryTree

$fsw = New-Object System.IO.FileSystemWatcher
$fsw.Path = $path
$fsw.IncludeSubdirectories = $true

$onChanged = Register-ObjectEvent $fsw 'Changed' -Action {
    if (-not (IsExcluded $Event.SourceEventArgs.FullPath)) {
        Add-Content -Path $logFilePath -Value "File Changed: $($Event.SourceEventArgs.FullPath) at $(Get-Date)"
    }
    Generate-DirectoryTree # Generate tree whenever a change occurs
}
$onCreated = Register-ObjectEvent $fsw 'Created' -Action {
    if (-not (IsExcluded $Event.SourceEventArgs.FullPath)) {
        Add-Content -Path $logFilePath -Value "File Created: $($Event.SourceEventArgs.FullPath) at $(Get-Date)"
    }
    Generate-DirectoryTree # Generate tree whenever a change occurs
}
$onDeleted = Register-ObjectEvent $fsw 'Deleted' -Action {
    if (-not (IsExcluded $Event.SourceEventArgs.FullPath)) {
        Add-Content -Path $logFilePath -Value "File Deleted: $($Event.SourceEventArgs.FullPath) at $(Get-Date)"
    }
    Generate-DirectoryTree # Generate tree whenever a change occurs
}
$onRenamed = Register-ObjectEvent $fsw 'Renamed' -Action {
    if (-not (IsExcluded $Event.SourceEventArgs.FullPath)) {
        Add-Content -Path $logFilePath -Value "File Renamed: $($Event.SourceEventArgs.FullPath) at $(Get-Date)"
    }
    Generate-DirectoryTree # Generate tree whenever a change occurs
}

$fsw.EnableRaisingEvents = $true

Write-Host "Monitoring changes in $path. Press [Enter] to exit."
[void][System.Console]::ReadLine()

# Clean up event handlers
Unregister-Event -SourceIdentifier $onChanged.SourceIdentifier
Unregister-Event -SourceIdentifier $onCreated.SourceIdentifier
Unregister-Event -SourceIdentifier $onDeleted.SourceIdentifier
Unregister-Event -SourceIdentifier $onRenamed.SourceIdentifier
