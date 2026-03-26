"""
Script 4: Validate Extraction Against Database
=================================================
Cross-checks the extracted JSON data against SQL database values.
This validates whether YOUR extraction pipeline produces trustworthy data.

Checks:
  1. Document classification: Does our verified_type match ATT_TYPE? (expect mismatches)
  2. Supplier name: Does extracted supplier match DB supplier?
  3. Price comparison: Does extracted price roughly match DB price?
  4. Item name quality: Is our canonical_item_name better than DB Item_Name?

Run: python 04_validate_extraction.py
"""

import os
import json
import glob
import pandas as pd
import pyodbc
from config import DB_CONFIG, OUTPUT_DIR

EXTRACTED_DIR = os.path.join(OUTPUT_DIR, "extracted_jsons")


def get_db_connection():
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
    )
    return pyodbc.connect(conn_str, timeout=30)


def load_extracted_jsons():
    """Load all extracted JSONs."""
    jsons = []
    for f in glob.glob(os.path.join(EXTRACTED_DIR, "*.json")):
        try:
            with open(f, "r") as fh:
                data = json.load(fh)
                data["_json_file"] = os.path.basename(f)
                jsons.append(data)
        except Exception as e:
            print(f"  [WARN] Could not load {f}: {e}")
    return jsons


def get_db_data_for_ras(conn, ras_ids):
    """Get DB data for the test RAS IDs."""
    ids_str = ",".join(str(x) for x in ras_ids)

    # Get BI view data
    bi_data = pd.read_sql(f"""
        SELECT
            PURCHASE_REQ_ID as ras_id,
            Item_Name,
            Purchase_Category,
            Sub_Category_Type,
            Supplier,
            Original_Item_Value_INR,
            Negotiated_Item_Value_INR
        FROM vw_get_ras_data_for_bidashboard
        WHERE PURCHASE_REQ_ID IN ({ids_str})
    """, conn)

    # Get attachment metadata
    att_data = pd.read_sql(f"""
        SELECT
            PURCHASE_ID as ras_id,
            ATTACHMENT_ID,
            FILES_NAME,
            ATT_TYPE,
            SUPPLIER_ID
        FROM purchase_attachments
        WHERE PURCHASE_ID IN ({ids_str})
    """, conn)

    return bi_data, att_data


def validate_document_classification(extracted_jsons, att_data):
    """Check: Does our classification match ATT_TYPE? How often do they disagree?"""
    results = []
    for ext in extracted_jsons:
        meta = ext.get("_metadata", {})
        doc_class = ext.get("document_classification", {})

        results.append({
            "ras_id": meta.get("ras_id"),
            "file_name": meta.get("file_name"),
            "att_type_from_db": meta.get("att_type_from_db"),
            "our_verified_type": doc_class.get("verified_type"),
            "our_confidence": doc_class.get("confidence"),
            "matches": doc_class.get("att_type_matches_content"),
            "reason": doc_class.get("classification_reason"),
        })

    df = pd.DataFrame(results)
    return df


def validate_prices(extracted_jsons, bi_data):
    """Check: Does our extracted price roughly match the DB price?"""
    results = []
    for ext in extracted_jsons:
        if not ext.get("is_quotation"):
            continue

        meta = ext.get("_metadata", {})
        ras_id = meta.get("ras_id")
        qd = ext.get("quotation_data", {})

        extracted_price = qd.get("quoted_price")
        extracted_supplier = qd.get("supplier_name")

        # Find matching DB record
        db_match = bi_data[bi_data["ras_id"].astype(str) == str(ras_id)]

        for _, db_row in db_match.iterrows():
            db_price_orig = db_row.get("Original_Item_Value_INR")
            db_price_neg = db_row.get("Negotiated_Item_Value_INR")
            db_supplier = db_row.get("Supplier")

            results.append({
                "ras_id": ras_id,
                "file_name": meta.get("file_name"),
                "extracted_supplier": extracted_supplier,
                "db_supplier": db_supplier,
                "supplier_match": (
                    str(extracted_supplier or "").lower() in str(db_supplier or "").lower()
                    or str(db_supplier or "").lower() in str(extracted_supplier or "").lower()
                ) if extracted_supplier and db_supplier else None,
                "extracted_price": extracted_price,
                "db_original_price": db_price_orig,
                "db_negotiated_price": db_price_neg,
                "price_in_range": (
                    abs(float(extracted_price) - float(db_price_orig)) / float(db_price_orig) < 0.2
                    if extracted_price and db_price_orig and float(db_price_orig) > 0
                    else None
                ),
            })

    return pd.DataFrame(results) if results else pd.DataFrame()


def validate_item_names(extracted_jsons, bi_data):
    """Compare: Is our canonical_item_name better than DB Item_Name?"""
    results = []
    for ext in extracted_jsons:
        if not ext.get("is_quotation"):
            continue

        meta = ext.get("_metadata", {})
        ras_id = meta.get("ras_id")
        qd = ext.get("quotation_data", {})
        our_name = qd.get("canonical_item_name")

        db_match = bi_data[bi_data["ras_id"].astype(str) == str(ras_id)]

        for _, db_row in db_match.iterrows():
            db_name = db_row.get("Item_Name")

            our_len = len(str(our_name)) if our_name else 0
            db_len = len(str(db_name)) if db_name else 0

            # Simple quality heuristic: longer, more specific name = better
            our_has_specs = any(c.isdigit() for c in str(our_name or ""))
            db_has_specs = any(c.isdigit() for c in str(db_name or ""))

            db_is_garbage = (
                db_name is None
                or str(db_name).strip() == ""
                or "as per" in str(db_name).lower()
                or "refer" in str(db_name).lower()
                or "attached" in str(db_name).lower()
            )

            results.append({
                "ras_id": ras_id,
                "db_item_name": db_name,
                "db_name_length": db_len,
                "db_has_specs": db_has_specs,
                "db_is_garbage": db_is_garbage,
                "our_canonical_name": our_name,
                "our_name_length": our_len,
                "our_has_specs": our_has_specs,
                "our_is_better": (our_len > db_len and our_has_specs) or db_is_garbage,
                "improvement": "MAJOR" if db_is_garbage and our_name else
                               "MINOR" if our_len > db_len * 1.5 else
                               "SAME" if abs(our_len - db_len) < 10 else "UNCLEAR",
            })

    return pd.DataFrame(results) if results else pd.DataFrame()


def main():
    print("=" * 60)
    print("SCRIPT 4: VALIDATE EXTRACTION AGAINST DATABASE")
    print("=" * 60)

    # Load extracted JSONs
    print("\n[1/5] Loading extracted JSONs...")
    extracted = load_extracted_jsons()
    print(f"  Loaded {len(extracted)} extracted documents")

    if len(extracted) == 0:
        print("  [ERROR] No extracted JSONs found. Run 03_extract_and_classify.py first.")
        return

    # Get RAS IDs
    ras_ids = set()
    for ext in extracted:
        rid = ext.get("_metadata", {}).get("ras_id")
        if rid:
            ras_ids.add(rid)

    # Get DB data
    print("\n[2/5] Fetching DB data for comparison...")
    conn = get_db_connection()
    bi_data, att_data = get_db_data_for_ras(conn, ras_ids)
    conn.close()
    print(f"  BI view: {len(bi_data)} rows | Attachments: {len(att_data)} rows")

    writer = pd.ExcelWriter(f"{OUTPUT_DIR}/validation_results.xlsx", engine="openpyxl")

    # Validation 1: Document classification
    print("\n[3/5] Validating document classification...")
    doc_val = validate_document_classification(extracted, att_data)
    if len(doc_val) > 0:
        doc_val.to_excel(writer, sheet_name="Doc_Classification", index=False)
        matches = doc_val["matches"].sum()
        mismatches = (~doc_val["matches"]).sum() if doc_val["matches"].dtype == bool else 0
        print(f"  ATT_TYPE matches our classification: {matches}")
        print(f"  ATT_TYPE MISMATCHES (we found different type): {mismatches}")

        if mismatches > 0:
            print("  Mismatch examples:")
            for _, row in doc_val[doc_val["matches"] == False].head(3).iterrows():
                print(f"    {row['file_name']}: DB='{row['att_type_from_db']}' → "
                      f"Actual='{row['our_verified_type']}' ({row['reason']})")

    # Validation 2: Price comparison
    print("\n[4/5] Validating extracted prices against DB...")
    price_val = validate_prices(extracted, bi_data)
    if len(price_val) > 0:
        price_val.to_excel(writer, sheet_name="Price_Validation", index=False)
        in_range = price_val["price_in_range"].sum()
        total_checked = price_val["price_in_range"].notna().sum()
        print(f"  Prices within 20% of DB value: {in_range}/{total_checked}")

        supplier_match = price_val["supplier_match"].sum()
        total_suppliers = price_val["supplier_match"].notna().sum()
        print(f"  Supplier name matches: {supplier_match}/{total_suppliers}")

    # Validation 3: Item name quality comparison
    print("\n[5/5] Comparing item name quality (ours vs DB)...")
    name_val = validate_item_names(extracted, bi_data)
    if len(name_val) > 0:
        name_val.to_excel(writer, sheet_name="Item_Name_Quality", index=False)
        major = len(name_val[name_val["improvement"] == "MAJOR"])
        minor = len(name_val[name_val["improvement"] == "MINOR"])
        same = len(name_val[name_val["improvement"] == "SAME"])
        print(f"  MAJOR improvement (DB was garbage, we extracted real name): {major}")
        print(f"  MINOR improvement (our name is more specific): {minor}")
        print(f"  SAME quality: {same}")

        if major > 0:
            print("\n  Examples of MAJOR improvement:")
            for _, row in name_val[name_val["improvement"] == "MAJOR"].head(3).iterrows():
                print(f"    DB: '{row['db_item_name']}'")
                print(f"    Ours: '{row['our_canonical_name']}'")
                print()

    writer.close()

    print(f"\n[DONE] Validation results saved to: {OUTPUT_DIR}/validation_results.xlsx")
    print("  Sheets: Doc_Classification, Price_Validation, Item_Name_Quality")
    print("\n  This tells you:")
    print("  - How unreliable ATT_TYPE really is (doc classification mismatches)")
    print("  - Whether your price extraction is accurate (price validation)")
    print("  - How much better your canonical names are vs DB Item_Name")


if __name__ == "__main__":
    main()
