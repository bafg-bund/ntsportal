


h <- curl::new_handle()
curl::handle_setheaders(h, "Accept" = "application/json")

req <- curl::curl_fetch_memory("https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/19700/property/inchikey", h)
jsonlite::prettify(rawToChar(req$content))

req <- curl::curl_fetch_memory("https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/smiles/C1=CC=C(C(=C1)CC(=O)O)NC2=C(C=CC=C2Cl)Cl/property/inchikey", h)
jsonlite::prettify(rawToChar(req$content))


# the compound classifications are found here
# https://pubchem.ncbi.nlm.nih.gov/classification/#hid=72
# you cant access these yet from the API
# write email to Emma to say what you are interessted in

