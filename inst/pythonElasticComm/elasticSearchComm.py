

import urllib3
import sys
import os
import json
from pathlib import Path
import importlib.util
from elasticsearch.dsl import Search
from elasticsearch import Elasticsearch
  
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def collectTable(tableName):
  client = getEsClient()
  s = Search(using=client, index=tableName)
  s = s[:10000]
  resp = s.execute()
  return resp.hits.to_list()

def import_module_from_path_importlib(module_name, module_path):
    """Imports a module from a specified path using importlib."""
    try:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    except Exception as e:
        print(f"Error importing module: {e}")
        return None

def createNewIndex(newIndexName, mappingType):
  client = getEsClient()
  indexMapping = getMapping(mappingType)
  resp = client.indices.create(index=newIndexName, body=indexMapping)
  print(resp)

def reindexDocs(oldIndex, newIndex):
  client = getEsClient()
  resp = client.reindex(
    source={"index": f"{oldIndex}"},
    dest={"index": f"{newIndex}"},
  )
  print(resp)

def updateAllDocs(indexName, script):
  client = getEsClient()
  resp = client.update_by_query(
    index=indexName,
    script=script,
    query={"match_all": {}},
  )
  print(resp)

def deleteIndex(indexName):
  client = getEsClient()
  resp = client.indices.delete(
    index=indexName,
  )
  print(resp)

def getMapping(mappingType):
  
  with open(os.path.join(os.getcwd(), "inst", "extdata", f"{mappingType}_index_mappings.json"), 'r') as f:
    indexMapping = json.load(f)
  return indexMapping

def getDbClient(username, password, hostUrl="https://elastic.dmz.bafg.de"):
  es_client = Elasticsearch(
        hosts=hostUrl,  # Elasticsearch endpoint
        basic_auth=(username, password),
        verify_certs=False
    )
  return es_client

def getEsClient():
  scriptDir = Path(__file__).parent.absolute()
  module_path = os.path.join(scriptDir, "ingest_sub.py")
  olesStuff = import_module_from_path_importlib("ingest_sub.py", module_path)
  credentials = olesStuff.get_user_credentials()
  es_client = olesStuff.connect_to_es(credentials)
  return es_client
