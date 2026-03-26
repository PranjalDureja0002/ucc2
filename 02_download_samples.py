"""
Script 2: Download Sample Attachments for Testing
====================================================
Picks the best RAS IDs for testing (from SQL + blob intersection)
and downloads their attachment files locally.

Run: python 02_download_samples.py
"""

import os
import re
import pymssql
import pandas as pd
from azure.storage.blob import ContainerClient
from config import BLOB_STORAGE, DB_CONFIG, OUTPUT_DIR, SAMPLE_SIZE

DOWNLOAD_DIR = os.path.join(OUTPUT_DIR, "sample_attachments")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def get_db_connection():
    return pymssql.connect(
        server=DB_CONFIG["server"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["username"],
        password=DB_CONFIG["password"],
        login_timeout=30,
    )


def get_blob_client():
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={BLOB_STORAGE['account_name']};"
        f"AccountKey={BLOB_STORAGE['account_key']};"
        f"EndpointSuffix=core.windows.net"
    )
    return ContainerClient.from_connection_string(
        conn_str, container_name=BLOB_STORAGE["container_attachments"]
    )


def find_best_ras_for_testing(conn):
    """Find RAS IDs that are good test candidates:
    - Recent (last 6 months)
    - Multiple suppliers with quotations
    - Has item description
    - Has multiple attachment types
    """
    print("  Querying SQL for good test candidates...")
    query = """
        SELECT TOP 50
            pa.PURCHASE_ID,
            COUNT(*) as num_attachments,
            COUNT(DISTINCT pa.SUPPLIER_ID) as num_suppliers,
            COUNT(DISTINCT pa.ATT_TYPE) as num_doc_types,
            STRING_AGG(DISTINCT COALESCE(pa.ATT_TYPE, 'NULL'), ', ') as doc_types,
            MAX(pa.UPLOADED_ON) as latest_upload
        FROM purchase_attachments pa
        WHERE pa.UPLOADED_ON >= DATEADD(MONTH, -6, GETDATE())
        GROUP BY pa.PURCHASE_ID
        HAVING COUNT(*) >= 3
           AND COUNT(DISTINCT pa.SUPPLIER_ID) >= 2
        ORDER BY COUNT(DISTINCT pa.SUPPLIER_ID) DESC, COUNT(*) DESC
    """
    df = pd.read_sql(query, conn)
    print(f"  Found {len(df)} candidates")
    return df


def get_ras_context(conn, ras_ids):
    """Get item details for the selected RAS IDs."""
    ids_str = ",".join(str(x) for x in ras_ids)
    query = f"""
        SELECT
            PURCHASE_REQ_ID,
            Item_Name,
            Purchase_Category,
            Sub_Category_Type,
            Supplier,
            Original_Item_Value_INR,
            Negotiated_Item_Value_INR
        FROM vw_get_ras_data_for_bidashboard
        WHERE PURCHASE_REQ_ID IN ({ids_str})
    """
    return pd.read_sql(query, conn)


def get_attachment_metadata(conn, ras_ids):
    """Get attachment details for the selected RAS IDs."""
    ids_str = ",".join(str(x) for x in ras_ids)
    query = f"""
        SELECT
            PURCHASE_ID,
            PURCHASE_DTL_ID,
            ATTACHMENT_ID,
            FILES_NAME,
            FILE_LOCATION,
            ATT_TYPE,
            SUPPLIER_ID,
            UPLOADED_ON
        FROM purchase_attachments
        WHERE PURCHASE_ID IN ({ids_str})
        ORDER BY PURCHASE_ID, UPLOADED_ON
    """
    return pd.read_sql(query, conn)


def download_blob_files(blob_client, ras_id, attachment_meta):
    """Download all blob files for a given RAS ID."""
    ras_dir = os.path.join(DOWNLOAD_DIR, f"RAS_{ras_id}")
    os.makedirs(ras_dir, exist_ok=True)

    downloaded = []

    # Method 1: Use FILE_LOCATION from DB if available
    for _, row in attachment_meta[attachment_meta["PURCHASE_ID"] == int(ras_id)].iterrows():
        file_loc = row.get("FILE_LOCATION")
        file_name = row.get("FILES_NAME", "unknown")

        if file_loc and str(file_loc).strip():
            blob_path = str(file_loc).strip()
            # Clean up path — remove leading slashes or container name if present
            if blob_path.startswith("/"):
                blob_path = blob_path[1:]
            if blob_path.startswith(BLOB_STORAGE["container_attachments"] + "/"):
                blob_path = blob_path[len(BLOB_STORAGE["container_attachments"]) + 1:]

            try:
                local_path = os.path.join(ras_dir, file_name)
                blob_data = blob_client.download_blob(blob_path)
                with open(local_path, "wb") as f:
                    f.write(blob_data.readall())
                downloaded.append({
                    "ras_id": ras_id,
                    "file_name": file_name,
                    "att_type": row.get("ATT_TYPE"),
                    "supplier_id": row.get("SUPPLIER_ID"),
                    "local_path": local_path,
                    "blob_path": blob_path,
                    "size_bytes": os.path.getsize(local_path),
                })
                print(f"    [OK] {file_name}")
            except Exception as e:
                print(f"    [FAIL] {file_name}: {e}")
                continue

    # Method 2: If FILE_LOCATION didn't work, try scanning by prefix
    if not downloaded:
        print(f"    FILE_LOCATION didn't work, scanning blob by prefix R_{ras_id}_...")
        prefix = f"R_{ras_id}_"
        try:
            for blob in blob_client.list_blobs(name_starts_with=prefix):
                file_name = os.path.basename(blob.name)
                local_path = os.path.join(ras_dir, file_name)
                try:
                    blob_data = blob_client.download_blob(blob.name)
                    with open(local_path, "wb") as f:
                        f.write(blob_data.readall())
                    downloaded.append({
                        "ras_id": ras_id,
                        "file_name": file_name,
                        "att_type": "unknown",
                        "supplier_id": None,
                        "local_path": local_path,
                        "blob_path": blob.name,
                        "size_bytes": os.path.getsize(local_path),
                    })
                    print(f"    [OK] {file_name}")
                except Exception as e:
                    print(f"    [FAIL] {file_name}: {e}")
        except Exception as e:
            print(f"    [FAIL] Prefix scan failed: {e}")

    return downloaded


def main():
    print("=" * 60)
    print("SCRIPT 2: DOWNLOAD SAMPLE ATTACHMENTS")
    print("=" * 60)

    # Step 1: Find best RAS candidates from SQL
    print("\n[1/4] Finding best test candidates...")
    conn = get_db_connection()
    candidates = find_best_ras_for_testing(conn)

    if len(candidates) == 0:
        print("  [ERROR] No candidates found. Check date range or table access.")
        return

    # Pick top N
    selected_ras_ids = candidates["PURCHASE_ID"].head(SAMPLE_SIZE).tolist()
    print(f"\n  Selected {len(selected_ras_ids)} RAS IDs for testing: {selected_ras_ids}")

    # Step 2: Get item context
    print("\n[2/4] Fetching item context from BI view...")
    context = get_ras_context(conn, selected_ras_ids)
    context.to_csv(f"{OUTPUT_DIR}/sample_ras_context.csv", index=False)
    print(f"  {len(context)} line items across {len(selected_ras_ids)} RAS records")
    for _, row in context.iterrows():
        print(f"    RAS {row['PURCHASE_REQ_ID']}: {str(row['Item_Name'])[:60]} | "
              f"{row['Supplier']} | ₹{row.get('Negotiated_Item_Value_INR', 'N/A')}")

    # Step 3: Get attachment metadata
    print("\n[3/4] Fetching attachment metadata...")
    att_meta = get_attachment_metadata(conn, selected_ras_ids)
    att_meta.to_csv(f"{OUTPUT_DIR}/sample_attachment_metadata.csv", index=False)
    print(f"  {len(att_meta)} attachments found")
    conn.close()

    # Step 4: Download files from blob storage
    print("\n[4/4] Downloading files from Azure Blob Storage...")
    blob_client = get_blob_client()
    all_downloaded = []

    for ras_id in selected_ras_ids:
        print(f"\n  RAS {ras_id}:")
        downloaded = download_blob_files(blob_client, ras_id, att_meta)
        all_downloaded.extend(downloaded)

    # Save download manifest
    if all_downloaded:
        manifest = pd.DataFrame(all_downloaded)
        manifest.to_csv(f"{OUTPUT_DIR}/download_manifest.csv", index=False)
        print(f"\n[DONE] Downloaded {len(all_downloaded)} files to: {DOWNLOAD_DIR}/")
        print(f"  Manifest saved to: {OUTPUT_DIR}/download_manifest.csv")
        print(f"\n  Next step: Run 03_extract_and_classify.py to process these files")
    else:
        print("\n[WARN] No files downloaded. Check blob storage access and FILE_LOCATION values.")


if __name__ == "__main__":
    main()
