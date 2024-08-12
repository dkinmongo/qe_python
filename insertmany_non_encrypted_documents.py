from pymongo import MongoClient
from faker import Faker
import random
from your_credentials import get_credentials

# Initialize Faker to generate random data
fake = Faker()

# Function to create a random patient document
def create_random_patient():
    return {
        "firstName": fake.first_name(),
        "lastName": fake.last_name(),
        "patientId": random.randint(10000000, 99999999),
        "address": fake.address(),
        "patientRecord": {
            "ssn": fake.ssn(),
            "billing": {
                "type": random.choice(["Visa", "MasterCard", "American Express"]),
                "number": fake.credit_card_number(),
            },
        },
        "medications": random.sample(["Atorvastatin", "Levothyroxine", "Lisinopril", "Omeprazole", "Metformin"], 2),
    }

# Retrieve credentials
credentials = get_credentials()

# MongoDB connection string
connection_string = credentials["MONGODB_URI"]

# Initialize the MongoDB client without encryption
unencrypted_client = MongoClient(connection_string)

# Define the database and collection names
non_encrypted_db_name = "test"
non_encrypted_coll_name = "patients_non_encrypted"

# Access the non-encrypted collection
non_encrypted_coll = unencrypted_client[non_encrypted_db_name][non_encrypted_coll_name]

# Insert 10,000 documents into the collection
patients = [create_random_patient() for _ in range(10000)]
non_encrypted_coll.insert_many(patients)

print("Inserted 10,000 patient records into the non-encrypted collection.")

# Clean up
unencrypted_client.close()

