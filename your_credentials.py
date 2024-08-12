import re

_credentials = {
    # Mongo Paths + URI
    "MONGODB_URI": "mongodb://localhost:30000,localhost:30001,localhost:30002/?replicaSet=rs0",
    "SHARED_LIB_PATH": "/usr/lib/mongo_crypt_v1.dylib",
    # AWS Credentials
    "AWS_ACCESS_KEY_ID": "xxx",
    "AWS_SECRET_ACCESS_KEY": "xxx",
    "AWS_KEY_REGION": "ap-northeast-2",
    "AWS_KEY_ARN": "xxx",
}


def check_for_placeholders():
    """check if credentials object contains placeholder values"""
    error_buffer = []
    placeholder_pattern = re.compile("^<.*>$")
    for key, value in _credentials.items():
        # check for placeholder text
        if placeholder_pattern.match(str(value)):
            error_message = (
                f"You must fill out the {key} field of your credentials object."
            )
            error_buffer.append(error_message)
        # check if value is empty
        elif not value:
            error_message = (
                f"The value for {key} is empty. Please enter something for this value."
            )
    # raise an error if errors in buffer
    if error_buffer:
        message = "\n".join(error_buffer)
        raise ValueError(message)


def get_credentials():
    """return credentials object and ensure it has been populated"""
    check_for_placeholders()
    return _credentials
