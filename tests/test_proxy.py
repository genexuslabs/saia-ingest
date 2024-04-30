import pytest

import requests
import json

from saia_ingest.utils import get_yaml_config

configuration = "config/sandbox.yaml"

@pytest.fixture
def config() -> dict:

  config = get_yaml_config(configuration)
  saia_level = config.get('saia', {})
  saia_base_url = saia_level.get('base_url', None)
  saia_api_token = saia_level.get('api_token', None)

  if not saia_base_url:
      raise ValueError("Missing $BASE_URL")

  if not saia_api_token:
      raise ValueError("Missing $SAIA_APITOKEN")

  return saia_level


def test_proxy(config):
  '''
  Sample GeneXus Enterprise AI Proxy testing
  https://wiki.genexus.com/enterprise-ai/wiki?19,GeneXus+Enterprise+AI+Proxy
  '''

  # generate an image with dall-e
  url = f"{config.get('base_url')}/proxy/openai/v1/images/generations"

  payload = {
    "model": "dall-e-2",
    "prompt": "a halloween pumpkin",
    "size": "256x256"
  }
  headers = {
    'Content-Type': 'application/json',
    'Authorization': f"Bearer {config.get('api_token')}",
  }

  result = requests.post(url, headers=headers, json=payload)

  assert result, "No result was returned"

  response = json.loads(result.content)

  assert response, "Invalid response"

  image_url = response.get('data')[0].get('url')

  assert image_url, "Invalid URL"



  # completions
  url = f"{config.get('base_url')}/proxy/openai/v1/chat/completions"

  payload = {
    "model": "gpt-3.5-turbo",
    "messages": [{
      "role": "user",
      "content": "Hi there"
    }]
  }
  headers = {
    'Content-Type': 'application/json',
    'Authorization': f"Bearer {config.get('api_token')}",
  }

  result = requests.post(url, headers=headers, json=payload)

  assert result, "No result was returned"

  response = json.loads(result.content)

  assert response, "Invalid response"

  reply = response.get('choices')[0].get('message').get('content')

  assert reply, "Invalid reply"

  return
