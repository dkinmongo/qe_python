import json
from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts
from your_credentials import get_credentials

app = Flask(__name__)

credentials = get_credentials()

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
    "dataKey5": keyVaultClient.find_one({"keyAltNames": "dataKey5"})["_id"],
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
            {
                "keyId": data_key_ids["dataKey5"],
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

auto_encryption = AutoEncryptionOpts(
    kms_providers,
    key_vault_namespace,
    encrypted_fields_map=encrypted_fields_map,
    crypt_shared_lib_path=credentials["SHARED_LIB_PATH"],
)

secure_client = MongoClient(connection_string, auto_encryption_opts=auto_encryption)
encrypted_coll = secure_client[encrypted_db_name][encrypted_coll_name]


# ============================================================
# Endpoints
# ============================================================

@app.route('/patients', methods=['POST'])
def add_patient():
    """Insert a patient document"""
    patient_data = request.json
    result = encrypted_coll.insert_one(patient_data)
    return jsonify({"status": "success", "inserted_id": str(result.inserted_id)}), 201


@app.route('/patients', methods=['GET'])
def get_patients():
    """Get patients by firstName or all"""
    first_name = request.args.get('firstName')
    if first_name:
        result = encrypted_coll.find_one({"firstName": first_name})
    else:
        result = list(encrypted_coll.find())
    return json.dumps(result, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/ssn/<ssn>', methods=['GET'])
def get_patient_by_ssn(ssn):
    """Equality query on encrypted ssn field"""
    result = encrypted_coll.find_one({"patientRecord.ssn": ssn})
    return json.dumps(result, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/ssns', methods=['GET'])
def get_patients_by_ssns():
    """Equality query with $in on encrypted ssn field"""
    ssns = request.args.getlist('ssns')
    results = list(encrypted_coll.find({"patientRecord.ssn": {"$in": ssns}}))
    return json.dumps(results, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/billing', methods=['GET'])
def get_patient_by_billing():
    """Query on non-queryable encrypted field (will fail)"""
    billing_info = request.json
    result = encrypted_coll.find_one({"patientRecord.billing": billing_info})
    return json.dumps(result, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/medications/<medication>', methods=['GET'])
def get_patient_by_medication(medication):
    """Query on non-queryable encrypted field (will fail)"""
    result = encrypted_coll.find_one({"medications": medication})
    return json.dumps(result, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/<patient_id>', methods=['PUT'])
def update_patient(patient_id):
    """Update patient by patientId (range query로 필터)"""
    update_data = request.json
    result = encrypted_coll.update_one(
        {"patientId": int(patient_id)},
        {"$set": update_data}
    )
    if result.matched_count == 0:
        return jsonify({"status": "failure", "message": "Patient not found"}), 404
    return jsonify({"status": "success", "modified_count": result.modified_count}), 200


@app.route('/patients/<patient_id>', methods=['DELETE'])
def delete_patient(patient_id):
    """Delete patient by patientId (range query로 필터)"""
    result = encrypted_coll.delete_one({"patientId": int(patient_id)})
    if result.deleted_count == 0:
        return jsonify({"status": "failure", "message": "Patient not found"}), 404
    return jsonify({"status": "success", "message": "Patient deleted"}), 200


# ============================================================
# [NEW] Range Query Endpoints
# ============================================================

@app.route('/patients/patientid/range', methods=['GET'])
def get_patients_by_patientid_range():
    """
    Range query on encrypted patientId field
    Usage: /patients/patientid/range?gte=10000000&lte=20000000
    """
    gte = request.args.get('gte', type=int)
    lte = request.args.get('lte', type=int)

    query = {}
    if gte is not None and lte is not None:
        query = {"patientId": {"$gte": gte, "$lte": lte}}
    elif gte is not None:
        query = {"patientId": {"$gte": gte}}
    elif lte is not None:
        query = {"patientId": {"$lte": lte}}
    else:
        return jsonify({"error": "Provide at least one of: gte, lte"}), 400

    results = list(encrypted_coll.find(query))
    print(f"Range query on patientId: {query}, found {len(results)} documents")
    return json.dumps(results, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/temperature/range', methods=['GET'])
def get_patients_by_temperature_range():
    """
    Range query on encrypted bodyTemperature field (double + precision)
    Usage: /patients/temperature/range?gte=37.0&lte=39.0
    Note: precision=1이므로 36.58은 36.5로, 37.82는 37.8로 매칭됨
    """
    gt = request.args.get('gt', type=float)
    lt = request.args.get('lt', type=float)
    gte = request.args.get('gte', type=float)
    lte = request.args.get('lte', type=float)

    range_filter = {}
    if gt is not None:
        range_filter["$gt"] = gt
    if lt is not None:
        range_filter["$lt"] = lt
    if gte is not None:
        range_filter["$gte"] = gte
    if lte is not None:
        range_filter["$lte"] = lte

    if not range_filter:
        return jsonify({"error": "Provide at least one of: gt, lt, gte, lte"}), 400

    query = {"bodyTemperature": range_filter}
    results = list(encrypted_coll.find(query))
    print(f"Range query on bodyTemperature: {query}, found {len(results)} documents")
    return json.dumps(results, default=str, ensure_ascii=False, indent=3)


@app.route('/patients/temperature/gt/<float:min_temp>', methods=['GET'])
def get_patients_fever(min_temp):
    """
    Simple range query: find patients with bodyTemperature > min_temp
    Usage: /patients/temperature/gt/37.5  (발열 환자 조회)
    """
    results = list(encrypted_coll.find({"bodyTemperature": {"$gt": min_temp}}))
    print(f"Patients with bodyTemperature > {min_temp}: {len(results)} found")
    return json.dumps(results, default=str, ensure_ascii=False, indent=3)


if __name__ == '__main__':
    app.run(debug=True)
