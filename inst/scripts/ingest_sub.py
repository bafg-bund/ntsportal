# Sub-functions for json file ingest
def read_json_files(json_path):
    """Read all json files at the specified path."""
    import gzip
    import pandas as pd
    import io

    # Get all valid json gz file paths at specified path
    json_files_gz = get_json_file_paths(json_path)

    # Open gz files and read decompressed content
    json_docs = []
    for file in json_files_gz:
        with gzip.open(file, 'rb') as f_in:
            # Read the decompressed content into memory
            decompressed_data = f_in.read()
            # Parse the decompressed content as json into a DataFrame
            json_docs.append(pd.read_json(io.BytesIO(decompressed_data)))

    return json_docs


def get_json_file_paths(path):
    """
    Gets all file paths at the specified path and filters for json files.
    If path is a directory, searches all subdirectories.
    """
    import os

    # Check if path is valid
    validate_path(path)

    # Check if provided path is a directory or a single file
    file_paths = []
    if os.path.isfile(path):
        file_paths = [path]
    else:  # If a directory is provided
        for folder, subfolders, filenames in os.walk(path):
            file_paths.extend(os.path.join(folder, filename) for filename in filenames)

    # Filter for json files
    json_files_gz = [file for file in file_paths if file.endswith('.gz')]

    if not json_files_gz:
        raise FileNotFoundError(f"No json files found at {path}.")

    return json_files_gz


def validate_path(path):
    """Validates if a path exists. Raises a `FileNotFoundError` if path doesn't exist."""
    import os.path
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path at {path} does not exist.")


def get_user_credentials():
    """
    Retrieves username and password from user config file (config.yml).
    Config file needs to be located at: /home/user/config.yml and needs to contain
    {'elastic_connect': {'user': <username>, 'pwd': <password>}}
    """
    import yaml
    import os

    with open(f'{os.getenv("HOME")}/config.yml', 'r') as f:
        user_config = yaml.safe_load(f)

    return user_config['default']['elastic_connect']


def connect_to_es(credentials):
    """Connects to Elasticsearch client with user credentials."""
    from elasticsearch import Elasticsearch

    # Create an Elasticsearch client using the user credentials
    es_client = Elasticsearch(
        hosts="https://elastic.dmz.bafg.de",  # Elasticsearch endpoint
        basic_auth=(credentials['user'], credentials['pwd']),
        verify_certs=False
    )
    return es_client


def update_import_time(json_docs):
    """Update the import time for the field 'date_import' in all json documents with single timestamp."""
    from datetime import datetime

    timestamp = datetime.now()
    date_import_now = timestamp.strftime('%Y-%m-%d %H:%M:%S')

    for df in json_docs:
        df['date_import'] = date_import_now

    return json_docs, timestamp


def ingest_json_docs(json_docs, es_client, timestamp, mapping_path):
    """Ingests json documents into Elasticsearch grouped by unique alias name. For each unique alias name a respective
     target index with timestamp is created and the alias is assigned."""

    # Get unique alias names
    unique_alias_names = (json_docs['dbas_alias_name'].unique())

    # Create dbas/nts indeces in Elasticsearch based on alias names
    target_index_alias_pairs = create_index_add_alias(unique_alias_names, es_client, timestamp, mapping_path)

    # Ingest data to target indeces in Elasticsearch
    ingest_data(target_index_alias_pairs, json_docs, es_client)
    
    return target_index_alias_pairs


def create_index_add_alias(unique_alias_names, es_client, timestamp, mapping_path):
    """
    Creates new dbas/nts indeces in Elasticsearch based on alias names found in the json documents and adds
    alias names to the created dbas/nts indeces.
    """
    import re
    import os
    import json
    from elasticsearch import exceptions

    # Create a new index from each unique alias name and add alias name
    target_index_alias_pairs = dict()
    for alias_name in unique_alias_names:

        # Get alias name type
        result = re.search(r'(_dbas|_nts)', alias_name)
        if not result:
            raise ValueError("Alias name must contain 'nts' or 'dbas'")
        alias_type = result.group(0)[1:]

        # Create index name from alias name
        version = timestamp.strftime("%y%m%d%H%M%S")
        index_name = re.sub(r'(ntsp)_(dbas|nts)', rf'\1_index_\2_v{version}', alias_name)

        # Add name pairs for target index and alias
        target_index_alias_pairs.update({alias_name: index_name})

        # Check if index already exists
        if not es_client.indices.exists(index=index_name):

            # Get correct index mapping body
            # mapping_path = f'{os.getenv("HOME")}/projects/ntsportal/inst/extdata/'
            with open(f'{mapping_path}/{alias_type}_index_mappings.json', 'r') as f:
                index_mapping_body = json.load(f)

            # Create index in Elasticsearch
            try:
                es_client.indices.create(index=index_name, body=index_mapping_body)
                print(f"Index '{index_name}' created successfully.")
            except exceptions.RequestError as e:
                # Errors related to Elasticsearch (e.g. index already exists or bad mapping)
                print(f"Failed to create index '{index_name}': {e.info}")
            except Exception as e:
                print(f"An unexpected error occurred: {str(e)}")

            # Add alias to the newly created index
            try:
                es_client.indices.put_alias(index=index_name, name=alias_name)
                print(f"Alias '{alias_name}' added to index '{index_name}'")
            except Exception as e:
                print(f"Failed to add alias: {str(e)}")

    return target_index_alias_pairs


def ingest_data(target_index_alias_pairs, json_docs, es_client):
    """Ingest data to target indeces in Elasticsearch."""
    from elasticsearch.helpers import streaming_bulk, BulkIndexError
    import math

    for alias_name, target_index_name in target_index_alias_pairs.items():
        # Filter the DataFrame for the current alias_name
        filtered_docs = json_docs[json_docs['dbas_alias_name'] == alias_name]

        # Function to drop NaN values during conversion from DataFrame to list of dictionaries
        def drop_nan_entries(row):
            return {key: value for key, value in row.items() if not (isinstance(value, float) and math.isnan(value))}

        # Transform DataFrame into list of dictionaries for ingest
        ingest_docs = [drop_nan_entries(row) for row in filtered_docs.to_dict(orient="records")]

        # Prepare actions as an iterable for streaming_bulk
        actions = ({"_index": target_index_name, "_source": doc} for doc in ingest_docs)

        try:
            # Index documents using streaming_bulk with tqdm
            for success, result in streaming_bulk(es_client, actions):
                action, response = result.popitem()
                if not success:
                    print(f"Failed to {action}: {response}")
        except BulkIndexError as e:
            print(f"BulkIndexError: {e.errors}")
            for error in e.errors:
                print(f"Document failed: {error}")
