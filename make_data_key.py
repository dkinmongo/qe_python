from pymongo import MongoClient, ASCENDING
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo.encryption import ClientEncryption
from bson.codec_options import CodecOptions
from bson.binary import STANDARD
from your_credentials import get_credentials

credentials = get_credentials()

provider = "aws"
kms_providers = {
    provider: {
        "accessKeyId": credentials["AWS_ACCESS_KEY_ID"],
        "secretAccessKey": credentials["AWS_SECRET_ACCESS_KEY"],
    }
}

master_key = {
    "region": credentials["AWS_KEY_REGION"],
    "key": credentials["AWS_KEY_ARN"],
}

connection_string = credentials["MONGODB_URI"]
key_vault_coll = "__keyVault"
key_vault_db = "qe"
key_vault_namespace = f"{key_vault_db}.{key_vault_coll}"

key_vault_client = MongoClient(connection_string)
key_vault_client.drop_database(key_vault_db)
key_vault_client[key_vault_db][key_vault_coll].create_index(
    [("keyAltNames", ASCENDING)],
    unique=True,
    partialFilterExpression={"keyAltNames": {"$exists": True}},
)

client = MongoClient(connection_string)
client_encryption = ClientEncryption(
    kms_providers,
    key_vault_namespace,
    client,
    CodecOptions(uuid_representation=STANDARD),
)

# DEK 생성 (5개)
data_key_id_1 = client_encryption.create_data_key(
    provider, master_key=master_key, key_alt_names=["dataKey1"]
)
print(f"dataKey1 created: {data_key_id_1}")

data_key_id_2 = client_encryption.create_data_key(
    provider, master_key=master_key, key_alt_names=["dataKey2"]
)
print(f"dataKey2 created: {data_key_id_2}")

data_key_id_3 = client_encryption.create_data_key(
    provider, master_key=master_key, key_alt_names=["dataKey3"]
)
print(f"dataKey3 created: {data_key_id_3}")

data_key_id_4 = client_encryption.create_data_key(
    provider, master_key=master_key, key_alt_names=["dataKey4"]
)
print(f"dataKey4 created: {data_key_id_4}")

data_key_id_5 = client_encryption.create_data_key(
    provider, master_key=master_key, key_alt_names=["dataKey5"]
)
print(f"dataKey5 created: {data_key_id_5}")

# Encrypted Collection 생성
encrypted_db_name = "test"
encrypted_coll_name = "patients"

encrypted_fields_map = {
    f"{encrypted_db_name}.{encrypted_coll_name}": {
        "fields": [
            {
                "keyId": data_key_id_1,
                "path": "patientId",
                "bsonType": "int",
                "queries": {
                    "queryType": "range",
                    "min": 10000000,
                    "max": 99999999,
                    "sparsity": 2,
                    "trimFactor": 6,
                    "contention": 8,
                },
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
            {
                # precision=1 → 36.58은 쿼리 시 36.5로 매칭 (저장은 36.58 그대로)
                "keyId": data_key_id_5,
                "path": "bodyTemperature",
                "bsonType": "double",
                "queries": {
                    "queryType": "range",
                    "min": 35.0,
                    "max": 42.0,
                    "precision": 1,
                    "sparsity": 2,
                    "trimFactor": 6,
                    "contention": 8,
                },
            },
        ],
    },
}

key_vault_namespace = "qe.__keyVault"
auto_encryption = AutoEncryptionOpts(
    kms_providers,
    key_vault_namespace,
    encrypted_fields_map=encrypted_fields_map,
    crypt_shared_lib_path=credentials["SHARED_LIB_PATH"],
)

secure_client = MongoClient(connection_string, auto_encryption_opts=auto_encryption)
secure_client.drop_database(encrypted_db_name)
encrypted_db = secure_client[encrypted_db_name]
encrypted_db.create_collection(encrypted_coll_name)
print("Created encrypted collection with range query support!")
