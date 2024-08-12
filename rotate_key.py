import json
from pymongo import MongoClient
from pymongo.encryption import ClientEncryption
from pymongo.encryption_options import AutoEncryptionOpts
from bson.json_util import dumps
from your_credentials import get_credentials
from bson.codec_options import CodecOptions
from bson.binary import STANDARD, UUID
# Retrieve credentials
credentials = get_credentials()


# MongoDB and Encryption Configuration
key_vault_db = "qe"
key_vault_coll = "__keyVault"
key_vault_namespace = "qe.__keyVault"

provider = "aws"
kms_providers = {
    provider: {
        "accessKeyId": credentials["AWS_ACCESS_KEY_ID"],
        "secretAccessKey": credentials["AWS_SECRET_ACCESS_KEY"],
    }
}

connection_string = credentials["MONGODB_URI"]
unencrypted_client = MongoClient(connection_string)

# Create a ClientEncryption instance
client_encryption = ClientEncryption(
    kms_providers,
    key_vault_namespace,
    unencrypted_client,
    CodecOptions(uuid_representation=STANDARD)    
)

# Define the data key IDs you want to rewrap
data_key_ids = [
    "dataKey1", 
    "dataKey3", 
    # Add more data key IDs as needed
]

# Define the new master key information for rewrapping
new_master_key = {
    "region": "ap-northeast-2",
    "key": "arn:aws:kms:ap-northeast-2:730335366609:key/1ab6dfe7-0cdb-4b4e-8c24-b0656d2146aa"
}

def rewrap_data_keys(data_key_ids):
    try:
        filter_condition = {"keyAltNames": {"$in": data_key_ids}}
        rewrap_result = client_encryption.rewrap_many_data_key(
            filter=filter_condition,
            provider=provider,
            master_key=new_master_key,
        )
        print("Successfully rewrapped data keys:")
        print(f"Rewrapped key IDs: {rewrap_result}")

    except Exception as e:
        print(f"Error during rewrapping: {str(e)}")

rewrap_data_keys(data_key_ids)

client_encryption.close()
unencrypted_client.close()

