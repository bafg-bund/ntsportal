# Main function for ingesting json files into Elasticsearch
from ingest_sub import *


def ingest(json_path):
    """
    Main processing logic for ingest of json files into Elasticsearch:
      1. Read json files
      2. Get user credentials # Todo
      3. Connect to Elasticsearch client
      4. Step through json files (batches)
        a. Create all target indeces based on unique aliases (groups) and add alias to every new index
        b. Ingest features into each target index (group)

    Args:
      json_path (str) : Path to single json file or directory with json files.
    
    Returns:
      all_index_alias_pairs (list of dicts) : All pairs of aliases and created indeces.
    """
    from tqdm import tqdm

    # Read json files
    json_docs = read_json_files(json_path)

    credentials = get_user_credentials()  # Todo: function to get user credentials. For now manual, because of keyring problems

    # Connect to the Elasticsearch (es) client
    es_client = connect_to_es(credentials)

    # Update import time for all json documents
    json_docs, timestamp = update_import_time(json_docs)

    # Ingest json documents
    all_index_alias_pairs = []
    for batch in tqdm(json_docs, total=len(json_docs), desc='Indexing features in json gz files'):
        all_index_alias_pairs.append(ingest_json_docs(batch, es_client, timestamp))

    return all_index_alias_pairs
