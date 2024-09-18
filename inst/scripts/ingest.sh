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
# TODO find a new home for the python file as part of the R package
SPATH="/home/Jewell/projects/gitlab/ntsportal/inst/scripts/nts_upload.py"


echo "----------- ingest.sh v$VERSION ----------------"
echo 'Submitted command:'
echo "$0" "$@"
echo "Submitted on $(date)"


if (($# < 3)); then
  echo "You must provide a config yaml file containing username and password, an index name and at least\
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

# Load python virtual environment, this is set for the Linux server
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

