import os

from robot.libraries.BuiltIn import BuiltIn


class QREnv:

    # Name of the bot. Will be used in email notifications
    BOT_NAME = "Kumari Letter Actions"

    # v1 currently in NCell, Prime, Civil, NMB. Rest are on v2
    PLATFORM_VERSION = 2
    NO_PLATFORM = True

    # IDENTIFIER = BuiltIn().get_variable_value("${identifier}")            # For platform v1
    # For platform v2
    IDENTIFIER = os.environ.get("identifier")
    # For testing on server via localhost
    # IDENTIFIER = "f2a2e5ee-5fcf-47ac-9ab0-9870647a225f"

    VERIFY_SSL = False
    DEBUG = True

    BASE_DIR = os.environ.get("ROBOT_ROOT")
    # Default to output directory
    ARTIFACT_DIR = os.environ.get("ROBOT_ARTIFACTS")
    # Downloaded files from storage bucket will be stored here
    DEFAULT_STORAGE_LOCATION = os.path.join(BASE_DIR, "storage_downloads")

    # ENVIRONMENTS - Should be retrieved dynamically from django env
    ENV_LOCAL = "LOCAL"  # Use for locahost
    ENV_QR_DEV = "QR_DEV"  # Use for internal dev server
    ENV_QR_UAT = "QR_UAT"  # Use for internal uat server
    ENV_UAT = "UAT"  # Use for client uat server
    ENV_PRODUCTION = "PRODUCTION"  # Use for client production

    # Platform URLS
    URL_LOCAL = "http://127.0.0.1:8000/api/v1"  # Use for locahost
    # Use for internal dev server
    URL_QR_DEV_URL = "http://13.58.117.7:8000/api/v1"
    # Use for internal uat server
    URL_QR_UAT_URL = "http://18.217.209.236/api/v1"
    # Use for client uat server
    URL_UAT_URL = "http://10.20.101.48/api/v1"
    # Use for client production
    URL_PROD = "http://10.20.101.48/api/v1"

    # Dictiory to pick Platform URL based on current Environment
    ENV_URL = {
        ENV_LOCAL: URL_LOCAL,
        ENV_QR_DEV: URL_QR_DEV_URL,
        ENV_QR_UAT: URL_QR_UAT_URL,
        ENV_UAT: URL_UAT_URL,
        ENV_PRODUCTION: URL_PROD,
    }

    # ENVIRONMENT = os.environ.get("ENVIRONMENT")
    ENVIRONMENT = ENV_LOCAL  # Use for locahost
    # ENVIRONMENT = ENV_PRODUCTION

    try:
        BASE_URL = ENV_URL[ENVIRONMENT]
    except Exception as e:
        BASE_URL = URL_LOCAL

    # Set required vaults, queues and storage buckets
    # Vault items are fetched. However, queues and storage buckets are only checked if they are accessible
    QUEUE_NAMES = []
    STORAGE_NAMES = []
    VAULT_NAMES = [
        "bot_database",
        "sqlserver_cred",
        "fuzzy_config",
        "apims_cred",
        "email_cred",
        "xtract_credentials",
    ]

    # Retrieved vault items are set in this dictionary
    VAULTS = {}
