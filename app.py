import json
from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts
from bson.json_util import dumps  # Import only dumps
from your_credentials import get_credentials

app = Flask(__name__)

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
unencryptedClient = MongoClient(connection_string)
keyVaultClient = unencryptedClient[key_vault_db][key_vault_coll]

data_key_ids = {
    "dataKey1": keyVaultClient.find_one({"keyAltNames": "dataKey1"})["_id"],
    "dataKey2": keyVaultClient.find_one({"keyAltNames": "dataKey2"})["_id"],
    "dataKey3": keyVaultClient.find_one({"keyAltNames": "dataKey3"})["_id"],
    "dataKey4": keyVaultClient.find_one({"keyAltNames": "dataKey4"})["_id"],
}

encrypted_db_name = "test"
encrypted_coll_name = "patients"

encrypted_fields_map = {
    f"{encrypted_db_name}.{encrypted_coll_name}": {
        "fields": [
            {
                "keyId": data_key_ids["dataKey1"],
                "path": "patientId",
                "bsonType": "int",
                "queries": {"queryType": "equality"},
            },
            {
                "keyId": data_key_ids["dataKey2"],
                "path": "medications",
                "bsonType": "array",
            },
            {
                "keyId": data_key_ids["dataKey3"],
                "path": "patientRecord.ssn",
                "bsonType": "string",
                "queries": {"queryType": "equality"},
            },
            {
                "keyId": data_key_ids["dataKey4"],
                "path": "patientRecord.billing",
                "bsonType": "object",
            },
        ],
    },
}

auto_encryption = AutoEncryptionOpts(
    kms_providers,
    key_vault_namespace,
    encrypted_fields_map=encrypted_fields_map,
    crypt_shared_lib_path=credentials["SHARED_LIB_PATH"],
)

secure_client = MongoClient(connection_string, auto_encryption_opts=auto_encryption)
encrypted_coll = secure_client[encrypted_db_name][encrypted_coll_name]

# Existing Endpoints

@app.route('/patients', methods=['GET'])
def get_patients():
    first_name = request.args.get('firstName')
    if first_name:
        result = encrypted_coll.find_one({"firstName": first_name})
    else:
        result = list(encrypted_coll.find())

    print("Retrieved Document:", result)
    return json.dumps(result, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/ssn/<ssn>', methods=['GET'])
def get_patient_by_ssn(ssn):
    result = encrypted_coll.find_one({"patientRecord.ssn": ssn})
    print("Retrieved Document:", result)
    return json.dumps(result, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/ssns', methods=['GET'])
def get_patients_by_ssns():
    ssns = request.args.getlist('ssns')
    results = list(encrypted_coll.find({"patientRecord.ssn": {"$in": ssns}}))
    print("Retrieved Document:", results)
    return json.dumps(results, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/medications/<medication>', methods=['GET'])
def get_patient_by_medication(medication):
    result = encrypted_coll.find_one({"medications": medication})
    print("Retrieved Document:", result)
    return json.dumps(result, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/billing', methods=['GET'])
def get_patient_by_billing():
    billing_info = request.json
    result = encrypted_coll.find_one({"patientRecord.billing": billing_info})
    print("Retrieved Document:", result)
    return json.dumps(result, default=str, ensure_ascii=False, indent=3)


# New Endpoint to Insert a Document
@app.route('/patients', methods=['POST'])
def add_patient():
    patient_data = request.json
    result = encrypted_coll.insert_one(patient_data)
    return jsonify({"status": "success", "inserted_id": str(result.inserted_id)}), 201

@app.route('/patients/<patient_id>', methods=['PUT'])
def update_patient(patient_id):
    update_data = request.json
    result = encrypted_coll.update_one(
        {"patientId": int(patient_id)},  # Use patientId as the filter
        {"$set": update_data}
    )
    
    if result.matched_count == 0:
        return jsonify({"status": "failure", "message": "Patient not found"}), 404
    else:
        return jsonify({"status": "success", "modified_count": result.modified_count}), 200

@app.route('/patients/<patient_id>', methods=['DELETE'])
def delete_patient(patient_id):
    result = encrypted_coll.delete_one({"patientId": int(patient_id)})

    if result.deleted_count == 0:
        return jsonify({"status": "failure", "message": "Patient not found"}), 404
    else:
        return jsonify({"status": "success", "message": "Patient deleted"}), 200




if __name__ == '__main__':
    app.run(debug=True)

