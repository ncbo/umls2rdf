#Folder to dump the RDF files.
OUTPUT_FOLDER = "output"

UMLS_VERSION = "2025AB"

#DB Config
DB_HOST = "localhost"
DB_NAME = "umls%s" % UMLS_VERSION.lower()
DB_USER = "umls"
DB_PASS = "umls"

# Define the base URI used to generate the concepts URI
UMLS_BASE_URI = "http://purl.bioontology.org/ontology/"

# Include the semantic type concepts for each Ontology file generated
INCLUDE_SEMANTIC_TYPES = True

# Only process ontologies updated in this UMLS release (MRSAB.IMETA == UMLS_VERSION)
PROCESS_ONLY_CURRENT_UMLS_VERSION = False
