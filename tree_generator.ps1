# Define the root directory (replace with your actual path)
$rootDirectory = "C:\Projects\Agent_web_UI"

# Define the output file
$outputFile = "directory_tree.txt"

# Define the folders to exclude
$excludeFolders = @(".git", ".vs", "__pycache__", "venv")  # Replace with actual folder names

# Function to generate the directory tree
function Write-DirectoryTree {
    param (
        [string]$path,
        [string]$prefix = ""
    )

    # Get the items in the current directory (both files and folders)
    $items = Get-ChildItem -Path $path

    foreach ($item in $items) {
        # Check if the current item is a folder and if it should be excluded
        if ($item.PSIsContainer) {
            if ($excludeFolders -contains $item.Name) {
                continue
            }

            # Write the current folder to the output file
            "$prefix├── $($item.Name)" | Out-File -Append -FilePath $outputFile

            # Recursively call the function for the item's subdirectory
            Write-DirectoryTree -path $item.FullName -prefix "$prefix│   "
        } else {
            # It's a file, write it to the output file
            "$prefix├── $($item.Name)" | Out-File -Append -FilePath $outputFile
        }
    }

    # Add a blank line for better readability
    "" | Out-File -Append -FilePath $outputFile
}

# Clear the output file before writing to it
Clear-Content -Path $outputFile

# Start writing the directory tree from the root directory
"$rootDirectory" | Out-File -FilePath $outputFile
Write-DirectoryTree -path $rootDirectory

Write-Host "Directory tree has been written to $outputFile"
Read-Host -Prompt "Press Enter  to exit"
