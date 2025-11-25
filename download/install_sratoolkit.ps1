# Élever le script si nécessaire
if (!([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltinRole] "Administrator"))
{
    Write-Warning "Ce script doit être exécuté en tant qu'administrateur."
    break
}

Write-Host "=== Vérification de Conda ==="

try {
    $condaVersion = conda --version 2>$null
    if ($condaVersion) {
        Write-Host "Conda est déjà installé : $condaVersion"
        $condaInstalled = $true
    }
} catch {
    $condaInstalled = $false
}

if (-not $condaInstalled) {
    Write-Host "Conda non trouvé, installation de Miniconda..."

    $minicondaUrl = "https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe"
    $installerPath = "$env:TEMP\MinicondaInstaller.exe"

    Write-Host "Téléchargement de Miniconda..."
    Invoke-WebRequest -Uri $minicondaUrl -OutFile $installerPath

    Write-Host "Installation silencieuse de Miniconda..."
    Start-Process -FilePath $installerPath -ArgumentList "/S /D=$env:USERPROFILE\Miniconda3" -Wait

    # Ajouter conda au PATH temporairement
    $env:Path = "$env:USERPROFILE\Miniconda3\Scripts;$env:USERPROFILE\Miniconda3;$env:Path"

    Remove-Item $installerPath
}

Write-Host "=== Conda prêt ==="
conda --version

Write-Host "=== Configuration des canaux ==="
conda config --add channels defaults
conda config --add channels bioconda
conda config --add channels conda-forge

Write-Host "=== Installation de SRA Toolkit ==="
conda install -y sra-tools

Write-Host "=== Vérification de SRA Toolkit ==="
try {
    $sraVersion = fasterq-dump --version
    Write-Host "SRA Toolkit installé avec succès ! Version : $sraVersion"
} catch {
    Write-Error "Échec de l'installation du SRA Toolkit."
}

