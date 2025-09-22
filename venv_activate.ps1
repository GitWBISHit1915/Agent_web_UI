cd 'C:\Projects\Agent_web_UI'

& .\venv\Scripts\Activate

$activateUvicorn = Read-Host "Do you want to activate the Uvicorn local host (y/n)?"

if ($activateUvicorn -eq 'y') {
	$port = Read-Host "Enter the port number (leave blank for default 8000)"
	if ([string]::IsNullOrWhiteSpace($port)) {
		$port = 8000
	}
	 Start-Process powershell -ArgumentList '-NoExit', '-Command', "uvicorn main:app --reload --port $port"
} else {
	Write-Host "Uvicorn activation skipped."
}

Read-Host -Prompt "Press Enter to exit the dungeon!"
