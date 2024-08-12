from pymongo import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo.encryption import ClientEncryption
from bson import ObjectId
import pprint
from your_credentials import get_credentials

credentials = get_credentials()

# MongoDB configuration
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

# Connect to the unencrypted client to access the key vault
unencrypted_client = MongoClient(connection_string)
keyVaultClient = unencrypted_client[key_vault_db][key_vault_coll]

# Retrieve data key IDs
data_key_id_1 = keyVaultClient.find_one({"keyAltNames": "dataKey1"})["_id"]
data_key_id_2 = keyVaultClient.find_one({"keyAltNames": "dataKey2"})["_id"]
data_key_id_3 = keyVaultClient.find_one({"keyAltNames": "dataKey3"})["_id"]
data_key_id_4 = keyVaultClient.find_one({"keyAltNames": "dataKey4"})["_id"]

# Define the encrypted fields map
encrypted_db_name = "test"
encrypted_coll_name = "patients"
encrypted_fields_map = {
    f"{encrypted_db_name}.{encrypted_coll_name}": {
        "fields": [
            {
                "keyId": data_key_id_1,
                "path": "patientId",
                "bsonType": "int",
                "queries": {"queryType": "equality"},
            },
            {
                "keyId": data_key_id_2,
                "path": "medications",
                "bsonType": "array",
            },
            {
                "keyId": data_key_id_3,
                "path": "patientRecord.ssn",
                "bsonType": "string",
                "queries": {"queryType": "equality"},
            },
            {
                "keyId": data_key_id_4,
                "path": "patientRecord.billing",
                "bsonType": "object",
            },
        ],
    },
}

# Configure auto-encryption
auto_encryption = AutoEncryptionOpts(
    kms_providers,
    key_vault_namespace,
    encrypted_fields_map=encrypted_fields_map,
    crypt_shared_lib_path=credentials["SHARED_LIB_PATH"],
)

# Start the secure client
secure_client = MongoClient(connection_string, auto_encryption_opts=auto_encryption)
encrypted_coll = secure_client[encrypted_db_name][encrypted_coll_name]

# Read from non-encrypted collection
non_encrypted_coll = unencrypted_client["test"]["patients_non_encrypted"]
patients_to_insert = []

# Fetch data from the non-encrypted collection
for patient in non_encrypted_coll.find():
    # Assuming the structure of the data matches the encrypted fields
    patients_to_insert.append(patient)

# Insert the patients into the encrypted collection
if patients_to_insert:
    result = encrypted_coll.insert_many(patients_to_insert)
    print(f"Inserted {len(result.inserted_ids)} documents into the encrypted collection.")
else:
    print("No patients found to insert.")

# Cleanup
unencrypted_client.close()
secure_client.close()

