# Main function for ingesting json files into Elasticsearch
from ingest_sub import *
from tqdm import tqdm

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def ingest(json_path, mapping_path):
    """
    Main processing logic for ingest of json files into Elasticsearch:
      1. Read json files
      2. Get user credentials
      3. Connect to Elasticsearch client
      4. Step through json files (batches)
        a. Create all target indeces based on unique aliases (groups) and add alias to every new index
        b. Ingest features into each target index (group)

    Args:
      json_path (str)     : Path to single json file or directory with json files.
      mapping_path (str)  : Path to mapping json files.
    
    Returns:
      all_index_alias_pairs (list of dicts) : All pairs of aliases and created indeces.
    """

    # Read json files
    json_docs = read_json_files(json_path)

    # Get user credentials (needs config.yml file in home folder)
    credentials = get_user_credentials()

    # Connect to the Elasticsearch (es) client
    es_client = connect_to_es(credentials)

    # Update import time for all json documents
    json_docs, timestamp = update_import_time(json_docs)

    # Ingest json documents
    all_index_alias_pairs = []
    for batch in tqdm(json_docs, total=len(json_docs), desc='Indexing features in json gz files'):
        all_index_alias_pairs.append(ingest_json_docs(batch, es_client, timestamp, mapping_path))

    return all_index_alias_pairs
