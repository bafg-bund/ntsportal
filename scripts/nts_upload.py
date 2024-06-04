# function is used to import json formated data into elastic search
# usage: python3 nts_upload.py ~/config.yml index_name import_data.json

# the first argument is the yml config file with username and password
# formated like this: 

# default:
#   elastic_connect:
#     user: 'username' 
#     pwd: 'password'

# the second argument is the index name to ingest to, the third argument is the 
# uncompressed json data.

# first you must set the python virtual environment with 
# source ~Jewell/venv/bin/activate
# to leave the virtual environment use
# deactivate

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import json
import tqdm
import time
import sys
import yaml

# set import time, will also be used as batch identifier

importTime = round(time.time())

def get_data_path():
  return(sys.argv[3])
  
def get_index_name():
  return(sys.argv[2])

def get_user():
  with open(sys.argv[1], 'r') as f:
    uc = yaml.safe_load(f)
    return uc['default']['elastic_connect']

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
      doc["date_import"] = importTime
      yield doc

def main():
  cred = get_user()
  client = Elasticsearch([{'host': 'elastic.dmz.bafg.de', 'port': 443, 'scheme':"https"}],
    http_auth=(cred['user'], cred['pwd']), verify_certs=False, timeout=30)
  
  print("Indexing documents...")
  progress = tqdm.tqdm(unit="docs", total=get_length())
  successes = 0
  for ok, action in streaming_bulk(client=client, index=get_index_name(), 
  actions=generate_actions(), max_retries=3, max_backoff=10):
      progress.update(1)
      successes += ok
  print("Indexed %d/%d documents" % (successes, get_length()))

if __name__ == "__main__":
    main()



# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
# ntsportal is free software: you can redistribute it and/or modify it under the 
# terms of the GNU General Public License as published by the Free Software 
# Foundation, either version 3 of the License, or (at your option) any 
# later version.
# 
# ntsportal is distributed in the hope that it will be useful, but WITHOUT ANY 
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along 
# with ntsportal. If not, see <https://www.gnu.org/licenses/>.
