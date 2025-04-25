
import re
import os
import json
from elasticsearch import exceptions
from elasticsearch.helpers import streaming_bulk, BulkIndexError
import math

def ingestRecords(recs, client, indexTimeStamp, indexMappingPath):
    """Ingests json documents into Elasticsearch grouped by unique alias name. For each unique alias name a respective
     target index with timestamp is created and the alias is assigned."""
    
    uniqueAliasNames = set(rec['dbas_alias_name'] for rec in recs)
    # Create dbas/nts indices in Elasticsearch based on alias names
    targetIndexAliasPairs = createIndexAddAlias(uniqueAliasNames, client, indexTimeStamp, indexMappingPath)

    # Ingest data to target indeces in Elasticsearch
    ingestToIndices(targetIndexAliasPairs, recs, client)
    
    return targetIndexAliasPairs


def createIndexAddAlias(uniqueAliasNames, client, indexTimeStamp, indexMappingPath):
    """
    Creates new dbas/nts indices in Elasticsearch based on alias names found in the json documents and adds
    alias names to the created dbas/nts indeces.
    """
    targetIndexAliasPairs = dict()
    for aliasName in uniqueAliasNames:

        # Get alias name type
        result = re.search(r'(_dbas|_nts)', aliasName)
        if not result:
            raise ValueError(f"Alias '{aliasName}' must contain 'nts' or 'dbas'")
        alias_type = result.group(0)[1:]

        # Create index name from alias name
        index_name = re.sub(r'(ntsp\d\d\.\d)_(dbas|nts)', f'\\1_index_\\2_v{indexTimeStamp}', aliasName)

        # Add name pairs for target index and alias
        targetIndexAliasPairs.update({aliasName: index_name})

        if not client.indices.exists(index=index_name):

            # Get correct index mapping body
            with open(f'{indexMappingPath}/{alias_type}_index_mappings.json', 'r') as f:
                index_mapping_body = json.load(f)

            try:
                client.indices.create(index=index_name, body=index_mapping_body)
                print(f"Index '{index_name}' created successfully.")
            except exceptions.RequestError as e:
                # Errors related to Elasticsearch (e.g. index already exists or bad mapping)
                print(f"Failed to create index '{index_name}': {e.info}")
            except Exception as e:
                print(f"An unexpected error occurred: {str(e)}")

            # Add alias to the newly created index
            try:
                client.indices.put_alias(index=index_name, name=aliasName)
                print(f"Alias '{aliasName}' added to index '{index_name}'")
            except Exception as e:
                print(f"Failed to add alias: {str(e)}")

    return targetIndexAliasPairs


def ingestToIndices(targetIndexAliasPairs, recs, client):
    """Ingest data to target indices in Elasticsearch."""
    for aliasName, targetIndexName in targetIndexAliasPairs.items():
        filteredRecs = [rec for rec in recs if rec['dbas_alias_name'] == aliasName]

        # Function to drop NaN values during conversion from DataFrame to list of dictionaries
        def drop_nan_entries(row):
            return {key: value for key, value in row.items() if not (isinstance(value, float) and math.isnan(value))}

        # Transform DataFrame into list of dictionaries for ingest
        ingestRecs = [drop_nan_entries(rec) for rec in filteredRecs]

        # Prepare actions as an iterable for streaming_bulk
        actions = ({"_index": targetIndexName, "_source": doc} for doc in ingestRecs)

        try:
            # Index documents using streaming_bulk
            for success, result in streaming_bulk(client, actions):
                action, response = result.popitem()
                if not success:
                    print(f"Failed to {action}: {response}")
        except BulkIndexError as e:
            print(f"BulkIndexError: {e.errors}")
            for error in e.errors:
                print(f"Document failed: {error}")
                
# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
