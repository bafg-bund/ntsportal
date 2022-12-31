from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import json
import tqdm
import time
import sys
import yaml

def get_user():
  with open(sys.argv[1], 'r') as f:
    uc = yaml.safe_load(f)
    return uc['default']['elastic_connect']

def get_data_path():
  return(sys.argv[3])
  
def get_index_name():
  esIndex = sys.argv[2]
  return(esIndex)

def get_length():
  with open(get_data_path(), mode="r") as f:
    json_docs = json.load(f)
    return len(json_docs)

def generate_actions():
  """Reads the file through and for each entry in the list
  yields a single document. This function is passed into the bulk()
  helper to create many documents in sequence.
  """
  with open(get_data_path(), mode="r") as f:
    json_docs = json.load(f)
    for doc in json_docs:
      #doc["date_import"] = round(time.time())
      yield doc

def main():
  cred = get_user()
  client = Elasticsearch([{'host': 'kibana.bafg.de', 'port': 9200,'scheme':"https"}],
    http_auth=(cred['user'], cred['pwd']), verify_certs=False)
  
  print("Indexing documents...")
  progress = tqdm.tqdm(unit="docs", total=get_length())
  successes = 0
  for ok, action in streaming_bulk(
      client=client, index=get_index_name(), actions=generate_actions(),
  ):
      progress.update(1)
      successes += ok
  print("Indexed %d/%d documents" % (successes, get_length()))

if __name__ == "__main__":
    main()
