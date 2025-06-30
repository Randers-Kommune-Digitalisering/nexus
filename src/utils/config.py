import os
from dotenv import load_dotenv


# loads .env file, will not overide already set enviroment variables (will do nothing when testing, building and deploying)
load_dotenv()


DEBUG = os.getenv('DEBUG', 'False').strip() in ['True', 'true']
TEST = os.getenv('TEST', 'False').strip() in ['True', 'true']
PORT = os.getenv('PORT', '8080').strip()
POD_NAME = os.getenv('POD_NAME', 'pod_name_not_set').strip()

# Delta
DELTA_TOP_ADM_UNIT_UUID = os.environ['DELTA_TOP_ADM_UNIT_UUID'].strip()
DELTA_URL = os.environ['DELTA_URL'].rstrip()
DELTA_CLIENT_ID = os.environ["DELTA_CLIENT_ID"].strip()
DELTA_CLIENT_SECRET = os.environ["DELTA_CLIENT_SECRET"].strip()
DELTA_REALM = '730'
DELTA_AUTH_URL = "https://idp.opus-universe.kmd.dk"

# NEXUS
NEXUS_URL = os.environ["NEXUS_URL"].strip()
NEXUS_CLIENT_ID = os.environ["NEXUS_CLIENT_ID"].strip()
NEXUS_CLIENT_SECRET = os.environ["NEXUS_CLIENT_SECRET"].strip()
NEXUS_REALM = os.environ["NEXUS_REALM"].strip()
NEXUS_AUTH_TYPE = os.environ["NEXUS_AUTH_TYPE"].strip()
