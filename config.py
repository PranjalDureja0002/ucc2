"""
RAS Extraction Pipeline — Configuration
==========================================
Azure Blob Storage + Azure OpenAI (LLM-only, no Doc Intelligence)
"""

# ============================================================
# AZURE BLOB STORAGE
# ============================================================
BLOB_STORAGE = {
    "account_name": "negotiationbenchmarking",
    "account_key": "YOUR_STORAGE_KEY",           # <-- UPDATE THIS
    "container_attachments": "negotiationbenchmarking-uat",
}

BLOB_BASE_URL = f"https://{BLOB_STORAGE['account_name']}.blob.core.windows.net/{BLOB_STORAGE['container_attachments']}"

# ============================================================
# AZURE OPENAI (the only AI service needed)
# ============================================================
AZURE_OPENAI = {
    "endpoint": "YOUR_AZURE_OPENAI_ENDPOINT",        # <-- UPDATE: e.g., https://your-resource.openai.azure.com/
    "key": "YOUR_AZURE_OPENAI_KEY",                   # <-- UPDATE THIS
    "deployment": "YOUR_DEPLOYMENT_NAME",             # <-- UPDATE: e.g., "gpt-51"
    "api_version": "2024-12-01-preview",
}

# ============================================================
# SQL DATABASE
# ============================================================
DB_CONFIG = {
    "server": "rasmastertable.database.windows.net",
    "database": "rasmaster",
    "username": "YOUR_USERNAME",                      # <-- UPDATE THIS
    "password": "YOUR_PASSWORD",                      # <-- UPDATE THIS
}

# ============================================================
# EXTRACTION SETTINGS
# ============================================================
SAMPLE_SIZE = 5              # Start small, validate, then scale
OUTPUT_DIR = "./extraction_output"
MAX_PDF_PAGES = 5            # Max pages to send per PDF (controls cost)
