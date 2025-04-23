# Main function for ingesting json files into Elasticsearch

from tqdm import tqdm
from pathlib import Path
import importlib.util
import urllib3
import os
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

def ingest(json_path, mapping_path, es_client):
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
    scriptDir = Path(__file__).parent.absolute()
    module_path = os.path.join(scriptDir, "ingest_sub.py")
    sub = import_module_from_path_importlib("ingest_sub.py", module_path)
    # Read json files
    json_docs = sub.read_json_files(json_path)

    # Update import time for all json documents
    json_docs, timestamp = sub.update_import_time(json_docs)

    # Ingest json documents
    all_index_alias_pairs = []
    for batch in tqdm(json_docs, total=len(json_docs), desc='Indexing features in json gz files'):
        all_index_alias_pairs.append(sub.ingest_json_docs(batch, es_client, timestamp, mapping_path))

    return all_index_alias_pairs
