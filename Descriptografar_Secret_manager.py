import os
from google.cloud import kms
import base64

# Defina a variável de ambiente para autenticação usando o arquivo de credenciais
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\MLABS\Downloads\credencials.json"

def decrypt_symmetric(
    project_id: str, location_id: str, key_ring_id: str, key_id: str, ciphertext: bytes
) -> kms.DecryptResponse:
    """
    Decrypt the ciphertext using the symmetric key.

    Args:
        project_id (string): Google Cloud project ID (e.g. 'my-project').
        location_id (string): Cloud KMS location (e.g. 'us-east1').
        key_ring_id (string): ID of the Cloud KMS key ring (e.g. 'my-key-ring').
        key_id (string): ID of the key to use (e.g. 'my-key').
        ciphertext (bytes): Encrypted bytes to decrypt.

    Returns:
        DecryptResponse: Response including plaintext.
    """
    # Create the KMS client.
    client = kms.KeyManagementServiceClient()

    # Build the key name.
    key_name = client.crypto_key_path(project_id, location_id, key_ring_id, key_id)

    # Optional: compute ciphertext's CRC32C.
    ciphertext_crc32c = crc32c(ciphertext)

    # Call the API to decrypt.
    decrypt_response = client.decrypt(
        request={
            "name": key_name,
            "ciphertext": ciphertext,
            "ciphertext_crc32c": ciphertext_crc32c,
        }
    )

    # Optional: integrity verification.
    if not decrypt_response.plaintext_crc32c == crc32c(decrypt_response.plaintext):
        raise Exception("The response was corrupted in transit.")

    print(f"Plaintext: {decrypt_response.plaintext.decode('UTF-8')}")
    return decrypt_response


def crc32c(data: bytes) -> int:
    """
    Calculates the CRC32C checksum of the provided data.
    """
    import crcmod  # type: ignore

    crc32c_fun = crcmod.predefined.mkPredefinedCrcFun("crc-32c")
    return crc32c_fun(data)


# A string fornecida (Base64)
encoded_str = "CiQASxdfweOqA8aaBx7A3Y9Zeb27GN3kNJ2N4yQSABCDbtRczBISPAAMqmeQZgTB2AEagf5aSmrdY56kK21vpZe9+U/q6xrHFQhWFf59s+bOg/g/DBgTv4oB8yqkQeKDNl9RMw=="

# Decodificando a string Base64 para obter os bytes criptografados
ciphertext = base64.b64decode(encoded_str)

# Chave KMS para descriptografar
project_id = "cdp-jhsf-prod"
location_id = "global"
key_ring_id = "jhsf-key-ring"
key_id = "kms-opera-key"

# Chamada para descriptografar o ciphertext usando Cloud KMS
decrypt_symmetric(project_id, location_id, key_ring_id, key_id, ciphertext)
