#!/bin/bash

# run alignment
printf -v thisDate '%(%Y-%m-%d)T' -1

echo "Alignment process started on $(date)"

Rscript script_assign_ufids.R &> ~/temp/ufid_$thisDate.log

# clear ufid db

Rscript clear_es_ufid_db.R

# create new ufid json

Rscript convert_ufid_to_json.R

# upload new db

source ~/venv/bin/activate

python3 ufid_ingest.py ~/config.yml g2_ufid1 ufid_db.json

deactivate

echo "updated new ufid-db on $(date)"

# assign ucids
echo "Assigning ucids"

Rscript script_assign_ucids.R &> "~/temp/ucid_$thisDate.log"

echo "Ucid alignment complete on $(date)"

echo done
