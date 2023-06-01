param(
    [string]$SnowflakeDeployment
)

# Convert the input to uppercase
$UppercaseDeployment = $SnowflakeDeployment.ToUpper()

# Decrypt the file based on the deployment option
switch ($UppercaseDeployment) {
    'AWS' {
        gpg --quiet --batch --yes --decrypt --passphrase="$env:DECRYPTION_PASSPHRASE" `
            --output profile.json ./profile.json.gpg
    }
    'GCP' {
        gpg --quiet --batch --yes --decrypt --passphrase="$env:DECRYPTION_PASSPHRASE" `
            --output profile.json ./profile_gcp.json.gpg
    }
    'AZURE' {
        gpg --quiet --batch --yes --decrypt --passphrase="$env:DECRYPTION_PASSPHRASE" `
            --output profile.json ./profile_azure.json.gpg
    }
    default {
        Write-Output "Invalid deployment option. Please enter 'AWS', 'AZURE', or 'GCP'."
    }
}
