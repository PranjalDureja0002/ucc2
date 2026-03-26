"""
Script 3: Extract & Classify Documents (LLM-Only)
====================================================
For each downloaded sample file:
  - PDF → convert pages to images → send to GPT (multimodal)
  - Excel → convert to CSV text → send to GPT
  - Images → send directly to GPT
  - One LLM call classifies doc type AND extracts structured fields

No Azure Document Intelligence needed. Just Azure OpenAI.

Run: python 03_extract_and_classify.py

Prerequisites: pip install pymupdf openpyxl openai pandas
"""

import os
import json
import time
import base64
import pandas as pd
import fitz  # PyMuPDF
from openai import AzureOpenAI
from config import AZURE_OPENAI, OUTPUT_DIR

DOWNLOAD_DIR = os.path.join(OUTPUT_DIR, "sample_attachments")
EXTRACTED_DIR = os.path.join(OUTPUT_DIR, "extracted_jsons")
os.makedirs(EXTRACTED_DIR, exist_ok=True)

MAX_PDF_PAGES = 5


def get_openai_client():
    return AzureOpenAI(
        azure_endpoint=AZURE_OPENAI["endpoint"],
        api_key=AZURE_OPENAI["key"],
        api_version=AZURE_OPENAI["api_version"],
    )


# ── File Converters ──

def pdf_to_images_base64(file_path, max_pages=MAX_PDF_PAGES):
    """Convert PDF pages to base64 PNG images."""
    doc = fitz.open(file_path)
    images = []
    total_pages = len(doc)
    for i in range(min(total_pages, max_pages)):
        page = doc[i]
        pix = page.get_pixmap(dpi=200)
        img_bytes = pix.tobytes("png")
        images.append(base64.b64encode(img_bytes).decode("utf-8"))
    doc.close()
    return images, total_pages


def excel_to_text(file_path):
    """Convert Excel to readable text."""
    try:
        xls = pd.ExcelFile(file_path)
        parts = []
        for sheet in xls.sheet_names[:5]:
            df = pd.read_excel(xls, sheet_name=sheet, nrows=100)
            if len(df) > 0:
                parts.append(f"=== Sheet: {sheet} ===")
                parts.append(df.to_string(index=False, max_colwidth=50))
        return "\n\n".join(parts)
    except Exception as e:
        return f"[Error reading Excel: {e}]"


def image_to_base64(file_path):
    with open(file_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def get_media_type(ext):
    return {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
            ".tiff": "image/tiff", ".bmp": "image/bmp"}.get(ext, "image/png")


# ── LLM Prompts ──

SYSTEM_PROMPT = """You are a procurement document analyst for an enterprise Requisition Application System (RAS).
You analyze procurement documents — quotations, MPBP forms, BER reports, SOW documents, emails, POs, and others.
Extract structured data precisely. If a field is not found, use null. Return ONLY valid JSON, no markdown, no backticks."""

EXTRACTION_PROMPT = """Analyze this procurement document.

FILE NAME: {file_name}
DATABASE LABEL (ATT_TYPE — may be incorrect): {att_type}

Return a JSON object:

{{
    "document_classification": {{
        "verified_type": "<one of: Quotation, MPBP, BER, SOW, 20K_Form, E_Auction, RFQ, Email, PO, Invoice, Catalog, Other>",
        "confidence": "<high/medium/low>",
        "att_type_from_db": "{att_type}",
        "att_type_matches_content": <true/false>,
        "classification_reason": "<brief reason>"
    }},
    "is_quotation": <true/false>,
    "quotation_data": {{
        "supplier_name": "<supplier who issued this quotation>",
        "canonical_item_name": "<most specific item name — include brand, model, size, tonnage, specs>",
        "item_model": "<model number if present>",
        "technical_specifications": {{
            "<spec_name>": "<spec_value>"
        }},
        "quoted_price": <total quoted price as number>,
        "currency": "<INR/USD/EUR>",
        "unit_price": <per-unit price if different>,
        "quantity": "<quantity quoted>",
        "payment_terms": "<e.g. 30% advance, 70% on delivery>",
        "delivery_terms": "<e.g. FOB Mumbai, 6 weeks>",
        "warranty": "<warranty terms>",
        "validity": "<quote validity period>",
        "quotation_date": "<date of quotation>",
        "quotation_reference": "<quotation number>"
    }},
    "additional_items": [
        {{
            "item_name": "<if multiple items>",
            "price": "<price>",
            "specs": "<brief specs>"
        }}
    ],
    "raw_summary": "<2-3 sentence summary>"
}}

RULES:
- If NOT a quotation, set is_quotation=false and quotation_data fields to null
- For canonical_item_name, be SPECIFIC — brand, model, tonnage, specs
- Multiple items: primary in quotation_data, rest in additional_items
- Return ONLY valid JSON"""


# ── LLM Callers ──

def call_llm_with_images(client, images_b64, file_name, att_type):
    content = [{"type": "text", "text": EXTRACTION_PROMPT.format(file_name=file_name, att_type=att_type)}]
    for img in images_b64:
        content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img}", "detail": "high"}})

    resp = client.chat.completions.create(
        model=AZURE_OPENAI["deployment"],
        messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": content}],
        temperature=0.1, max_tokens=3000,
    )
    return parse_response(resp)


def call_llm_with_text(client, text, file_name, att_type):
    prompt = EXTRACTION_PROMPT.format(file_name=file_name, att_type=att_type)
    prompt += f"\n\nDOCUMENT CONTENT:\n---\n{text[:12000]}\n---"

    resp = client.chat.completions.create(
        model=AZURE_OPENAI["deployment"],
        messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}],
        temperature=0.1, max_tokens=3000,
    )
    return parse_response(resp)


def call_llm_with_single_image(client, img_b64, media_type, file_name, att_type):
    content = [
        {"type": "text", "text": EXTRACTION_PROMPT.format(file_name=file_name, att_type=att_type)},
        {"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{img_b64}", "detail": "high"}},
    ]
    resp = client.chat.completions.create(
        model=AZURE_OPENAI["deployment"],
        messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": content}],
        temperature=0.1, max_tokens=3000,
    )
    return parse_response(resp)


def parse_response(resp):
    c = resp.choices[0].message.content.strip()
    if c.startswith("```json"): c = c[7:]
    if c.startswith("```"): c = c[3:]
    if c.endswith("```"): c = c[:-3]
    return json.loads(c.strip())


# ── Main ──

def main():
    print("=" * 60)
    print("SCRIPT 3: EXTRACT & CLASSIFY (LLM-ONLY)")
    print("=" * 60)

    manifest_path = f"{OUTPUT_DIR}/download_manifest.csv"
    if not os.path.exists(manifest_path):
        print(f"[ERROR] No manifest at {manifest_path}. Run 02_download_samples.py first.")
        return

    manifest = pd.read_csv(manifest_path)
    print(f"\n  {len(manifest)} files to process")

    client = get_openai_client()
    print("  [OK] Azure OpenAI client ready\n")

    results = []

    for idx, row in manifest.iterrows():
        file_path = row["local_path"]
        file_name = row["file_name"]
        ras_id = row["ras_id"]
        att_type = row.get("att_type", "unknown")
        ext = os.path.splitext(file_name)[1].lower()

        print(f"  [{idx+1}/{len(manifest)}] RAS {ras_id}: {file_name} ({ext})")

        if not os.path.exists(file_path):
            print(f"    [SKIP] File not found")
            results.append({"ras_id": ras_id, "file_name": file_name, "status": "not_found"})
            continue

        try:
            # ── Route by file type ──
            if ext == ".pdf":
                images, total = pdf_to_images_base64(file_path)
                print(f"    PDF: {len(images)}/{total} pages → GPT")
                extracted = call_llm_with_images(client, images, file_name, att_type)

            elif ext in (".xlsx", ".xls"):
                text = excel_to_text(file_path)
                print(f"    Excel: {len(text)} chars → GPT")
                extracted = call_llm_with_text(client, text, file_name, att_type)

            elif ext in (".png", ".jpg", ".jpeg", ".tiff", ".bmp"):
                img_b64 = image_to_base64(file_path)
                print(f"    Image → GPT")
                extracted = call_llm_with_single_image(client, img_b64, get_media_type(ext), file_name, att_type)

            elif ext in (".doc", ".ppt", ".pptx", ".msg"):
                print(f"    [SKIP] {ext} — needs conversion, not supported yet")
                results.append({"ras_id": ras_id, "file_name": file_name, "status": "unsupported", "ext": ext})
                continue

            else:
                print(f"    [SKIP] Unsupported: {ext}")
                results.append({"ras_id": ras_id, "file_name": file_name, "status": "unsupported", "ext": ext})
                continue

            # Add metadata
            extracted["_metadata"] = {
                "ras_id": str(ras_id),
                "file_name": file_name,
                "file_extension": ext,
                "att_type_from_db": att_type,
                "supplier_id_from_db": row.get("supplier_id"),
                "extraction_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            # Save JSON
            safe = file_name.replace("/", "_").replace("\\", "_")
            with open(os.path.join(EXTRACTED_DIR, f"RAS_{ras_id}_{safe}.json"), "w") as f:
                json.dump(extracted, f, indent=2, default=str)

            # Print key findings
            dc = extracted.get("document_classification", {})
            is_q = extracted.get("is_quotation", False)
            print(f"    [OK] Type: {dc.get('verified_type','?')} | "
                  f"ATT_TYPE match: {dc.get('att_type_matches_content','?')} | "
                  f"Quotation: {is_q}")

            if is_q and extracted.get("quotation_data"):
                qd = extracted["quotation_data"]
                print(f"         Supplier: {qd.get('supplier_name','?')}")
                print(f"         Item: {str(qd.get('canonical_item_name','?'))[:70]}")
                print(f"         Price: {qd.get('currency','')} {qd.get('quoted_price','?')}")

            results.append({
                "ras_id": ras_id, "file_name": file_name, "ext": ext, "status": "success",
                "verified_type": dc.get("verified_type"),
                "att_type_from_db": att_type,
                "att_type_matches": dc.get("att_type_matches_content"),
                "is_quotation": is_q,
                "canonical_item_name": extracted.get("quotation_data", {}).get("canonical_item_name") if is_q else None,
                "quoted_price": extracted.get("quotation_data", {}).get("quoted_price") if is_q else None,
                "supplier_name": extracted.get("quotation_data", {}).get("supplier_name") if is_q else None,
            })

        except json.JSONDecodeError as e:
            print(f"    [FAIL] JSON parse: {e}")
            results.append({"ras_id": ras_id, "file_name": file_name, "status": "json_error"})
        except Exception as e:
            print(f"    [FAIL] {e}")
            results.append({"ras_id": ras_id, "file_name": file_name, "status": "error", "error": str(e)})

        time.sleep(2)  # Rate limiting

    # Summary
    summary = pd.DataFrame(results)
    summary.to_csv(f"{OUTPUT_DIR}/extraction_results.csv", index=False)

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    ok = summary[summary["status"] == "success"]
    print(f"  Total: {len(manifest)} | Success: {len(ok)}")
    if "is_quotation" in summary.columns:
        print(f"  Quotations: {summary['is_quotation'].sum()}")
    if "att_type_matches" in summary.columns:
        mis = ok[ok["att_type_matches"] == False]
        print(f"  ATT_TYPE mismatches: {len(mis)}")

    print(f"\n  JSONs: {EXTRACTED_DIR}/")
    print(f"  Next: python 04_validate_extraction.py")


if __name__ == "__main__":
    main()
