from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import json
import tqdm
import time
import sys
import yaml

def get_data_path():
  return(sys.argv[2])
  

def get_index_name():
  esIndex = sys.argv[1]
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
  client = Elasticsearch([
    'http://kevin.jewell:g23456@10.140.73.204:9200/'
  ])
  
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
