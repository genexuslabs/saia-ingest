'''

import pinecone
from typing import List, Iterator
from ast import literal_eval
# Ignore unclosed SSL socket warnings - optional in case you get these errors
import warnings

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning) 


def get_vectorstore_index(index_name=None):
    return pinecone.Index(index_name)


'''
