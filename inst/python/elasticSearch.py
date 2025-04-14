
import urllib3
import sys
import os
import json
  
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

def getEsClient():
  sys.path.append(os.path.join(os.getcwd(), "inst", "extdata", "scripts"))
  import ingest_sub
  
  credentials = ingest_sub.get_user_credentials()
  es_client = ingest_sub.connect_to_es(credentials)
  return es_client
