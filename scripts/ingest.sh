#!/bin/bash

# Script to upload multiple json files to an index


# ingest.sh config-file index-name [json files...]

# the first argument is the yml config file with username and password
# formated like this: 

# default:
#   elastic_connect:
#     user: 'username' 
#     pwd: 'password'

# the second argument is the index name to ingest to, further arguments are the 
# compressed or uncompressed json data (.json or .json.gz).

# To pass a list of json files from the find command use a subshell:
# ./ingest.sh ~/config.yml g2_dbas_upb $(find -L ~/messdaten -name 221024*.json)
# or use the -exec option of find
# find -L ~/messdaten -name 221024*.json -exec ./ingest.sh ~/config.yml g2_dbas_upb '{}' ';'

# Define constants
VERSION="2022-10-25"
SPATH="/home/Jewell/projects/ntsportal/scripts/nts_upload.py"


echo "----------- ingest.sh v$VERSION ----------------"
echo 'Submitted command:'
echo "$0" "$@"
echo "Submitted on $(date)"


if (($# < 3)); then
  echo "You must provide a config yaml file with username and password, an index name and at least\
  one json file"
  exit 1
fi

# load and test arguments

configFile="$1"

if ! [[ "$configFile" =~ .*\.ya?ml$ ]]; then
  echo "First argument must be a .yml file"
  exit 1
fi

shift

indexName="$1"
if ! [[ "$indexName" =~ ^g2_.* || "$indexName" =~ ^ntsp_.* ]]; then
  echo "Second argument is not a valid index name"
  exit 1
fi

shift

# load python virtual environment
source ~Jewell/venv/bin/activate

# loop through json files 

for jsonFile in "$@"; do
  if [[ "$jsonFile" =~ .*\.json$ && -e "$jsonFile" ]]; then
    echo "Ingesting $jsonFile"
    python3 $SPATH $configFile $indexName $jsonFile
    if (($? == 0)); then
      echo "Ingest successful for $jsonFile"
    else
      echo "Ingest unsuccessful for $jsonFile"
    fi
  elif [[ "$jsonFile" =~ .*\.json\.gz$ && -e "$jsonFile" ]]; then
    echo "Decompressing $jsonFile"
    gunzip $jsonFile
    newJson=$(echo $jsonFile | sed 's/\.gz//')
    python3 $SPATH $configFile $indexName $newJson
    
    if (($? == 0)); then
      echo "Ingest successful for $newJson"
    else
      echo "Ingest unsuccessful for $newJson"
    fi
    
    echo "Compressing $newJson"
    gzip $newJson
    
  else
    echo "$jsonFile not a json or not found"
  fi
done

# Close python virtual environment 
deactivate

echo "-- End of script. Thank you for choosing ingest.sh today, take care and goodbye. --"
