# RAS Extraction Pipeline (LLM-Only)

Your own document extraction pipeline. Downloads sample attachments from Azure Blob Storage, sends them directly to Azure OpenAI (multimodal) for classification and structured extraction. No Doc Intelligence needed.

## How it works

- **PDF** → PyMuPDF converts pages to images → images sent to GPT (multimodal vision)
- **Excel** → openpyxl/pandas converts to text → text sent to GPT
- **Images** → sent directly to GPT
- **One LLM call** classifies the document type AND extracts structured fields

## Setup

```bash
pip install -r requirements.txt
```

Edit `config.py`:
1. `BLOB_STORAGE.account_key` — Azure Blob Storage key
2. `AZURE_OPENAI.endpoint`, `.key`, `.deployment` — your Azure OpenAI deployment
3. `DB_CONFIG.username`, `.password` — SQL Server credentials

## Run in Order

```bash
python 01_blob_analysis.py          # What's in blob storage (file types, sizes, coverage)
python 02_download_samples.py       # Download best test RAS attachments
python 03_extract_and_classify.py   # LLM extracts and classifies each file
python 04_validate_extraction.py    # Cross-check against SQL database
```

## What Each Script Does

| Script | Input | Output |
|--------|-------|--------|
| 01_blob_analysis | Azure Blob container | File type distribution, RAS coverage, size stats |
| 02_download_samples | SQL (best RAS) + Blob (files) | Downloaded files + metadata CSVs |
| 03_extract_and_classify | Downloaded files + Azure OpenAI | One JSON per file with classification + extracted data |
| 04_validate_extraction | Extracted JSONs + SQL database | Validation report: ATT_TYPE mismatches, price checks, item name quality |

## JSON Output Schema

Each file produces a JSON like:

```json
{
    "document_classification": {
        "verified_type": "Quotation",
        "confidence": "high",
        "att_type_matches_content": true
    },
    "is_quotation": true,
    "quotation_data": {
        "supplier_name": "Engel Austria GmbH",
        "canonical_item_name": "Engel Victory 3550/400 Tech PRO 400T Injection Moulding Machine",
        "technical_specifications": { "tonnage": "400T", "type": "Horizontal" },
        "quoted_price": 8623842,
        "currency": "INR",
        "payment_terms": "30% advance, 70% on delivery"
    }
}
```

This JSON powers everything downstream: ingestion embeddings, P/L1/L2, dashboard KPIs.
