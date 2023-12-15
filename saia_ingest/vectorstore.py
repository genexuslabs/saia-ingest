
import pinecone
from typing import List, Iterator
from ast import literal_eval
# Ignore unclosed SSL socket warnings - optional in case you get these errors
import warnings

import pinecone

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning) 


def initialize_vectorstore_connection(api_key=None, environment=None):
    pinecone.init(api_key=api_key, environment=environment)

def get_vectorstore_index(index_name=None):
    return pinecone.Index(index_name)


