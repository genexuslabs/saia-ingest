"""
Maintain custom header naming convention using kebab-case format.
"""


class DefaultHeaders:
    AUTHORIZATION = 'Authorization'
    JSON_CONTENT_TYPE = 'application/json'
    SOURCE = 'X-Saia-Source'
    REQUEST_ID = 'X-Saia-Req-Id'
    PARENT_REQUEST_ID = 'X-Saia-Parent-Req-Id'
    PROXY_MOCK = 'X-Saia-Proxy-Mock'
    ASYNC = 'X-Saia-Async'
    CACHE_ENABLED = 'X-Saia-Cache-Enabled'
    TRACE_ID = 'X-Saia-Trace-Id'
    USER_ID = 'X-Saia-User-Id'
    TIMEOUT = 5000  # ms
    MAX_RETRIES = 2


class DefaultVectorStore:
    CHUNK_SIZE = 1000
    CHUNK_OVERLAP = 100
    SEPARATORS = ['\n\n', '\n', ' ', '']
    BATCH_SIZE = 100
    DISTANCE_METRIC = "cosine"


class DefaultSearch:
    K = 4
    K_INCREMENT = 1
    SCORE_THRESHOLD = 0.0
    RETURN_SOURCE_DOCS = True
    SEARCH_TYPE = "SIMILARITY"
    LAMBDA = 0.1
    QUERY_COUNT = 5
    USE_ORIGINAL_QUERY = False


class DefaultLLM:
    PROVIDER = "openai"
    TEMPERATURE = 0
    VERBOSE = False
    CACHE = False
    USE_PROXY = True
    MODEL_NAME = 'gpt-3.5-turbo'
    SUMMARIZE_MODEL_NAME = 'gpt-3.5-turbo-16k'
    STREAM = False
    MAX_TOKENS = 1000
    TOP_P = 1
    FREQUENCY_PENALTY = 0
    PRESENCE_PENALTY = 0
    N = 1


class Defaults:
    PACKAGE_DESCRIPTION = "GeneXus Enterprise AI"
    PACKAGE_URL = "https://github.com/genexuslabs/saia-ingest/blob/main/README.md"
