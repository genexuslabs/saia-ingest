import logging
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential

logging.getLogger('azure').setLevel(logging.WARNING)

class KeyVaultClient:
    def __init__(self, vault_name: str, client_id: str, client_secret: str, tenant_id: str):
        kv_uri = f"https://{vault_name}.vault.azure.net/"
        credential = ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id
        )
        self.client = SecretClient(vault_url=kv_uri, credential=credential)

    def get_secret(self, secret_name: str) -> str:
        secret = self.client.get_secret(secret_name)
        return secret.value
