# Copyright (c) [2024] GeneXus S.A.
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import pytest
import logging
from dotenv import load_dotenv

from saia_ingest.assistant_utils import get_assistants, get_assistant

load_dotenv()

@pytest.fixture
def config(configuration) -> dict:

    saia_level = configuration.get('saia', {})
    saia_base_url = saia_level.get('base_url', None)
    saia_api_token = saia_level.get('api_token', None)

    if not saia_base_url:
        raise ValueError("Missing $BASE_URL")

    if not saia_api_token:
        raise ValueError("Missing $SAIA_APITOKEN")

    return saia_level


def test_assistants(config):

    base_url = config.get('base_url')
    api_token = config.get('api_token')
    result = get_assistants(base_url, api_token)

    assert result, "No result was returned"

    for item in result:
        assistant_id = item['assistantId']

        assistant = get_assistant(base_url, api_token, assistant_id)

        name = assistant.get('assistantName')
        type = assistant.get('assistantType')
        status = assistant.get('assistantStatus')

        assert name, "Assistant name is None"
        assert type, "Assistant Type is None"
        assert status, "Assistant Status is None"

        detail = f"Id: {assistant_id} Name: {name} Type: {type} Status: {status}"
        logging.info(detail)
        break

    return

