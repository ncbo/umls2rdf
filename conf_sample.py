#Folder to dump the RDF files.
OUTPUT_FOLDER = "output"

#DB Config
DB_HOST = "your-host"
DB_NAME = "umls2015ab"
DB_USER = "your db user"
DB_PASS = "your db pass"

UMLS_VERSION = "2015ab"

# Define the base URI used to generate the concepts URI
UMLS_BASE_URI = "http://purl.bioontology.org/ontology/"

# Include the semantic type concepts for each Ontology file generated
INCLUDE_SEMANTIC_TYPES = True

# Remove duplication of triples when loading on cuis
LOAD_CUIS_REMOVE_DUPLICATION = True
