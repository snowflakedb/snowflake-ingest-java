#!/bin/sh

# Decrypt the file
# --batch to prevent interactive command --yes to assume "yes" for questions

# Input one argument
snowflake_deployment=$1

# Convert the input to uppercase
uppercase_deployment=$(echo $snowflake_deployment | tr '[:lower:]' '[:upper:]')

if [ $uppercase_deployment = 'AWS' ]; then
  gpg --quiet --batch --yes --decrypt --passphrase="$DECRYPTION_PASSPHRASE" \
  --output profile.json ./profile.json.gpg
elif [ $uppercase_deployment = 'GCP' ]; then
  gpg --quiet --batch --yes --decrypt --passphrase="$DECRYPTION_PASSPHRASE" \
    --output profile.json ./profile_gcp.json.gpg
elif [ $uppercase_deployment = 'AZURE' ]; then
  gpg --quiet --batch --yes --decrypt --passphrase="$DECRYPTION_PASSPHRASE" \
    --output profile.json ./profile_azure.json.gpg
else
  echo "Invalid deployment option. Please enter 'AWS', 'AZURE', or 'GCP'."
fi