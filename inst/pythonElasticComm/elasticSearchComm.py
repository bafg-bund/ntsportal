

import urllib3
from elasticsearch import Elasticsearch
  
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def getDbClient(username, password, hostUrl):
  es_client = Elasticsearch(
        hosts=hostUrl,
        basic_auth=(username, password),
        verify_certs=False
  )
  return es_client


