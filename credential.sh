# Fetch credentials from Vault and store them in variables
EMAIL=$(vault kv get -field=email secret/data/screener)
PASSWORD=$(vault kv get -field=password secret/data/screener)
 
# Save credentials to files
echo "$EMAIL" > credentials/email.txt
echo "$PASSWORD" > credentials/password.txt
 
# Print the credentials for debugging (ensure this is secure and not used in production)
echo "Fetched Email: $EMAIL"
echo "Fetched Password: $PASSWORD"

