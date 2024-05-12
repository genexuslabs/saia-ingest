import pytest
import os

@pytest.fixture(scope="session")
def configuration():
    conf = {
        "saia": {
            "base_url": os.environ.get('BASE_URL'),
            "api_token": os.environ.get('API_TOKEN'),
            "profile": os.environ.get('ASSISTANT_NAME')
        }
    }
    return conf