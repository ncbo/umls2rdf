UMLS_VERSION = "2025AB"

# Folder to dump the RDF files.
OUTPUT_FOLDER = "output/%s" % UMLS_VERSION.upper()

# DB Config
DB_HOST = "localhost"
DB_NAME = "umls%s" % UMLS_VERSION.lower()
DB_USER = "umls"
DB_PASS = "umls"
MYSQL_HOME = "/usr"

# Include the semantic type concepts for each Ontology file generated
INCLUDE_SEMANTIC_TYPES = True

# Define the base URI used to generate the concepts URI
UMLS_BASE_URI = "http://purl.bioontology.org/ontology/"

# Only process ontologies updated in this UMLS release (MRSAB.IMETA == UMLS_VERSION)
PROCESS_ONLY_CURRENT_UMLS_VERSION = False

# Pipeline config
UMLS_API_KEY = "your umls api key"

# Optional: final umls download directory, e.g. data/umls
# UMLS_DOWNLOAD_DIR = "data/umls"

# Optional: directory for extracted full UMLS contents
# UMLS_EXTRACT_DIR = "data/umls-extracted"

# Optional: working directory for pipeline state and patched loader script
# PIPELINE_WORK_DIR = "data/pipeline"

# Optional: pipeline log file path
# PIPELINE_LOG_FILE = "data/pipeline/pipeline.log"
