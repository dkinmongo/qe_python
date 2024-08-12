# Queryable Encryption with Python and Flask

This project demonstrates the implementation of Queryable Encryption using Python and Flask. It includes basic RESTful API operations to handle encrypted data.

## Table of Contents
- [Pre-requisites](#pre-requisites)
- [Running the Application](#running-the-application)
- [Data Encryption Key (DEK) Generation](#generate-data-encryption-keys-deks)
- [CRUD Operations](#crud-operations)
  - [Insert](#insert-patient-data)
  - [Find](#find-patient-data)
  - [Update](#update-patient-data)
  - [Delete](#delete-patient-data)
- [Key Rotation](#key-rotation)
- [Additional Information](#additional-information)
  - [Plaintext Data Migration Samples](#plaintext-data-migration-samples)
  
## Pre-requisites

Before running the application, ensure you have the following information stored in `your_credentials.py`:

- **AWS KMS CMK (Customer Master Key) information**: Required for encryption and decryption operations.
- **SHARED_LIB_PATH**: Path to the encrypted schema map library. Example: `/usr/lib/mongo_crypt_v1.dylib`.
- **Python Environment Initial Setup**: To set up the environment and install the necessary dependencies, run the following commands:
```bash
conda create -n qetest pip python=3.9
conda activate test
pip install -r requirements.txt
```

## Running the Application

The application is built with Flask. You can start the server by running:

```bash
python app.py
```

## Generate Data Encryption Keys (DEKs)
Execute the following script to generate Data Encryption Keys (DEKs):

```bash
python make_data_key.py
```

## CRUD Operations

### Insert Patient Data
To insert a new patient record, use the following `curl` command:

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
    "firstName": "Jon",
    "lastName": "Doe",
    "patientId": 12345678,
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

### Find Patient Data
- **Get a patient by first name:**

```bash
curl "http://127.0.0.1:5000/patients?firstName=Jon"
```

- **Get a patient by SSN:**

```bash
curl "http://127.0.0.1:5000/patients/ssn/987-65-4320"
```

- **Get a patient by SSNs:**

```bash
curl "http://127.0.0.1:5000/patients/ssns?ssns=987-65-4320&ssns=527-35-3702"
```

- **Get a patient by medication:**

```bash
curl "http://127.0.0.1:5000/patients/medications/Atorvastatin"
```

- **Get a patient by billing information:**

```bash
curl -X GET -H "Content-Type: application/json" -d '{"type": "Visa","number": "4111111111111111"}' http://127.0.0.1:5000/patients/billing
```

### Update Patient Data
To update a patient's record, including queryable fields, use the following command:

```bash
curl -X PUT -H "Content-Type: application/json" -d '{
    "patientRecord.ssn" : "527-35-3702",
    "medications": ["Atorvastatin", "test"],
    "patientRecord.billing": {
            "type": "MasterCard",
            "number": "5111 1111 1111 1111"
    }
}' http://127.0.0.1:5000/patients/12345678
```

### Delete Patient Data
To delete a patient's record, including queryable fields, use the following command:

```bash
curl -X DELETE http://127.0.0.1:5000/patients/12345678
```

## Key Rotation
To rotate the encryption key, execute the following script:

```bash
python rotate_key.py
```

## Additional Information

### Plaintext Data Migration Samples

- **Loading Plaintext Data:**

  ```bash
  python insertmany_non_encrypted_documents.py
  ```
- **Reading Plaintext Data and Reloading as Encrypted:**

  ```bash
  python insertmany_non_encrypted_documents.py
  ```

