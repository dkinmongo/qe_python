
# Queryable Encryption with Python and Flask (v1.1)

A demo project implementing MongoDB Queryable Encryption (QE) with Python and Flask.
Includes both Equality queries (MongoDB 7.0+) and Range queries (MongoDB 8.0+).

## Table of Contents
- [Pre-requisites](#pre-requisites)
- [Encrypted Fields Configuration](#encrypted-fields-configuration)
- [Data Encryption Key (DEK) Generation](#generate-data-encryption-keys-deks)
- [Running the Application](#running-the-application)
- [CRUD Operations](#crud-operations)
  - [Insert](#insert-patient-data)
  - [Find — Equality Query](#find-patient-data--equality-query)
  - [Find — Range Query (8.0+)](#find-patient-data--range-query-80)
  - [Update](#update-patient-data)
  - [Delete](#delete-patient-data)
- [Key Rotation](#key-rotation)
- [Plaintext Data Migration](#plaintext-data-migration)
- [Version History](#version-history)

## Pre-requisites

### Requirements
- **MongoDB**: 8.0+ (Enterprise Advanced or Atlas)
- **Python**: 3.9+
- **PyMongo**: 4.7+
- **crypt_shared**: Download separately from the MongoDB Enterprise download page

### Initial Setup

```bash
conda create -n qetest pip python=3.9
conda activate qetest
pip install -r requirements.txt
```

### Credentials Configuration

Set the following information in `your_credentials.py`:

```python
_credentials = {
    # MongoDB URI
    "MONGODB_URI": "mongodb://localhost:30000,localhost:30001,localhost:30002/?replicaSet=rs0",
    # crypt_shared library path
    "SHARED_LIB_PATH": "/path/to/mongo_crypt_v1.dylib",
    # AWS KMS Credentials (for CMK management)
    "AWS_ACCESS_KEY_ID": "xxx",
    "AWS_SECRET_ACCESS_KEY": "xxx",
    "AWS_KEY_REGION": "ap-northeast-2",
    "AWS_KEY_ARN": "xxx",
}
```

Note: In addition to AWS KMS, Google Cloud KMS, Azure Key Vault, and KMIP-compliant solutions (e.g., HashiCorp Vault) are also supported.


## Encrypted Fields Configuration

This demo encrypts 5 fields and demonstrates both equality and range queries.

| Field | bsonType | queryType | Notes |
|---|---|---|---|
| `patientId` | int | **range** | min=10000000, max=99999999 |
| `medications` | array | none (encrypt only) | Not queryable |
| `patientRecord.ssn` | string | **equality** | Exact match queries |
| `patientRecord.billing` | object | none (encrypt only) | Not queryable |
| `bodyTemperature` | **double** | **range** | min=35.0, max=42.0, **precision=1** |

- `patientId`: Demonstrates range queries on an int type
- `bodyTemperature`: Demonstrates range queries on a **double type with the precision parameter**
  - With precision=1, a stored value of 36.58°C is matched as 36.5 in queries (the stored value remains 36.58)


## Generate Data Encryption Keys (DEKs)

Generate DEKs and create the encrypted collection:

```bash
python make_data_key.py
```

This script performs the following:
- Creates 5 DEKs (dataKey1~5) in the Key Vault (`qe.__keyVault`)
- Creates the `test.patients` encrypted collection with the encrypted fields configuration above
- Automatically creates metadata collections (`enxcol_.patients.esc`, `enxcol_.patients.ecoc`)


## Running the Application

Start the Flask server:

```bash
python app.py
```

The server starts at `http://127.0.0.1:5000`.


## CRUD Operations

### Insert Patient Data

Insert a document including the `bodyTemperature` field:

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
    "firstName": "Jon",
    "lastName": "Doe",
    "patientId": 12345678,
    "bodyTemperature": 36.5,
    "address": "157 Electric Ave.",
    "patientRecord": {
        "ssn": "987-65-4320",
        "billing": {
            "type": "Visa",
            "number": "4111111111111111"
        }
    },
    "medications": ["Atorvastatin", "Levothyroxine"]
}' http://127.0.0.1:5000/patients
```

Additional test data:

```bash
# Patient with fever
curl -X POST -H "Content-Type: application/json" \
-d '{
    "firstName": "Jane",
    "lastName": "Smith",
    "patientId": 23456789,
    "bodyTemperature": 38.2,
    "address": "742 Evergreen Terrace",
    "patientRecord": {
        "ssn": "123-45-6789",
        "billing": {"type": "MasterCard", "number": "5222222222222222"}
    },
    "medications": ["Metformin"]
}' http://127.0.0.1:5000/patients

# Patient with high fever
curl -X POST -H "Content-Type: application/json" \
-d '{
    "firstName": "Bob",
    "lastName": "Johnson",
    "patientId": 34567890,
    "bodyTemperature": 39.8,
    "address": "123 Main St",
    "patientRecord": {
        "ssn": "555-66-7777",
        "billing": {"type": "Visa", "number": "4333333333333333"}
    },
    "medications": ["Lisinopril", "Omeprazole"]
}' http://127.0.0.1:5000/patients
```


### Find Patient Data — Equality Query

```bash
# Find by name (unencrypted field)
curl "http://127.0.0.1:5000/patients?firstName=Jon"

# Find by SSN (equality query on encrypted field)
curl "http://127.0.0.1:5000/patients/ssn/987-65-4320"

# Find by multiple SSNs ($in)
curl "http://127.0.0.1:5000/patients/ssns?ssns=987-65-4320&ssns=123-45-6789"
```

The following queries target **encrypted fields with queryType: none** and will result in errors:

```bash
# medications (queryType: none) — not queryable, returns error
curl "http://127.0.0.1:5000/patients/medications/Atorvastatin"

# billing (queryType: none) — not queryable, returns error
curl -X GET -H "Content-Type: application/json" \
-d '{"type": "Visa","number": "4111111111111111"}' \
http://127.0.0.1:5000/patients/billing
```


### Find Patient Data — Range Query (8.0+)

**patientId (int, range)**:

```bash
# Patients with patientId between 10000000 and 20000000
curl "http://127.0.0.1:5000/patients/patientid/range?gte=10000000&lte=20000000"

# Patients with patientId >= 20000000
curl "http://127.0.0.1:5000/patients/patientid/range?gte=20000000"
```

**bodyTemperature (double, range, precision=1)**:

```bash
# Patients with temperature between 37.0 and 39.0 (suspected fever)
curl "http://127.0.0.1:5000/patients/temperature/range?gte=37.0&lte=39.0"

# Patients with temperature below 38.0
curl "http://127.0.0.1:5000/patients/temperature/range?lt=38.0"

# Patients with temperature above 37.5 — fever screening shortcut API
# (precision=1: a stored value of 37.58 is matched as 37.5)
curl "http://127.0.0.1:5000/patients/temperature/gt/37.5"
```


### Update Patient Data

```bash
# Update including queryable fields
curl -X PUT -H "Content-Type: application/json" -d '{
    "patientRecord.ssn": "527-35-3702",
    "bodyTemperature": 37.8,
    "medications": ["Atorvastatin", "test"],
    "patientRecord.billing": {
        "type": "MasterCard",
        "number": "5111 1111 1111 1111"
    }
}' http://127.0.0.1:5000/patients/12345678
```

Note: When updating encrypted fields, entries are **appended** (not replaced) to the metadata collections (ESC, ECOC). Repeated updates will cause metadata size to grow continuously.


### Delete Patient Data

```bash
curl -X DELETE http://127.0.0.1:5000/patients/12345678
```

Note: Metadata collection entries are **not automatically removed** on delete. Run `compactStructuredEncryptionData()` manually when ECOC size exceeds 1GB.


## Key Rotation

Rotate the CMK (Customer Master Key) and rewrap DEKs:

```bash
python rotate_key.py
```

- The DEK plaintext value remains unchanged, so existing encrypted data is not affected (Zero Downtime)
- Do not immediately delete the Old CMK from KMS after rewrap — verify all DEKs have been successfully rewrapped first


## Plaintext Data Migration

Sample scripts for migrating existing plaintext data to an encrypted collection.

### 1. Generate Plaintext Test Data (10,000 documents)

```bash
python insertmany_non_encrypted_documents.py
```

Generates random patient data including the `bodyTemperature` field (35.5–40.5°C, 1 decimal place).


### 2. Migrate Plaintext Data → Encrypted Collection

```bash
python insertmany_encrypted_from_non_encryted.py
```

- Reads plaintext data from `test.patients_non_encrypted` and inserts into `test.patients` via an encrypted client
- The driver's `encrypted_fields_map` ensures designated fields are automatically encrypted
- Inserts in batches of 1,000 documents (to avoid the BSON 16MB limit caused by QE metadata overhead)
- Preserves original `_id` values to maintain document identity

