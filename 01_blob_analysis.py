"""
Script 1: Blob Storage Analysis
==================================
Connects to Azure Blob Storage and analyzes:
  - Total files, total size
  - File type distribution (PDF, Excel, images, etc.)
  - RAS IDs covered
  - Files per RAS
  - Sample file paths

Run: python 01_blob_analysis.py
"""

import os
import re
from collections import Counter, defaultdict
from azure.storage.blob import ContainerClient
from config import BLOB_STORAGE, OUTPUT_DIR

os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_container_client():
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={BLOB_STORAGE['account_name']};"
        f"AccountKey={BLOB_STORAGE['account_key']};"
        f"EndpointSuffix=core.windows.net"
    )
    return ContainerClient.from_connection_string(
        conn_str, container_name=BLOB_STORAGE["container_attachments"]
    )


def extract_ras_id(blob_name):
    """Extract RAS ID from blob path like R_237910_2025/..."""
    match = re.match(r"R_(\d+)_\d{4}/", blob_name)
    return match.group(1) if match else None


def get_extension(blob_name):
    """Get file extension."""
    _, ext = os.path.splitext(blob_name)
    return ext.lower() if ext else "(no extension)"


def format_size(size_bytes):
    """Format bytes to human readable."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def main():
    print("=" * 60)
    print("SCRIPT 1: BLOB STORAGE ANALYSIS")
    print("=" * 60)

    client = get_container_client()

    print(f"\n[1/5] Listing blobs in container: {BLOB_STORAGE['container_attachments']}")
    print("  (This may take a few minutes for large containers...)")

    total_files = 0
    total_size = 0
    ext_counter = Counter()
    ext_sizes = defaultdict(int)
    ras_files = defaultdict(list)
    sample_paths = []
    size_buckets = Counter()  # <1MB, 1-10MB, 10-50MB, 50-100MB, >100MB

    for blob in client.list_blobs():
        total_files += 1
        total_size += blob.size

        ext = get_extension(blob.name)
        ext_counter[ext] += 1
        ext_sizes[ext] += blob.size

        ras_id = extract_ras_id(blob.name)
        if ras_id:
            ras_files[ras_id].append({
                "name": blob.name,
                "size": blob.size,
                "ext": ext,
            })

        # Size buckets
        mb = blob.size / (1024 * 1024)
        if mb < 1:
            size_buckets["< 1 MB"] += 1
        elif mb < 10:
            size_buckets["1-10 MB"] += 1
        elif mb < 50:
            size_buckets["10-50 MB"] += 1
        elif mb < 100:
            size_buckets["50-100 MB"] += 1
        else:
            size_buckets["> 100 MB"] += 1

        # Collect samples
        if total_files <= 20:
            sample_paths.append({"path": blob.name, "size": format_size(blob.size), "ext": ext})

        if total_files % 10000 == 0:
            print(f"  Scanned {total_files:,} files...")

    # ── Results ──
    print(f"\n[2/5] SUMMARY")
    print(f"  Total files:    {total_files:,}")
    print(f"  Total size:     {format_size(total_size)}")
    print(f"  Distinct RAS:   {len(ras_files):,}")

    print(f"\n[3/5] FILE TYPE DISTRIBUTION")
    for ext, count in ext_counter.most_common(15):
        pct = count / total_files * 100
        avg_size = ext_sizes[ext] / count
        print(f"  {ext:15s} {count:>8,} ({pct:5.1f}%)  avg size: {format_size(avg_size)}")

    print(f"\n[4/5] SIZE DISTRIBUTION")
    for bucket in ["< 1 MB", "1-10 MB", "10-50 MB", "50-100 MB", "> 100 MB"]:
        count = size_buckets.get(bucket, 0)
        pct = count / total_files * 100 if total_files > 0 else 0
        print(f"  {bucket:12s} {count:>8,} ({pct:5.1f}%)")

    print(f"\n[5/5] FILES PER RAS")
    if ras_files:
        counts = [len(v) for v in ras_files.values()]
        print(f"  Min:    {min(counts)}")
        print(f"  Avg:    {sum(counts)/len(counts):.1f}")
        print(f"  Median: {sorted(counts)[len(counts)//2]}")
        print(f"  Max:    {max(counts)}")

    # ── Save to CSV ──
    import csv

    # File type distribution
    with open(f"{OUTPUT_DIR}/blob_file_types.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["extension", "count", "pct", "total_size", "avg_size"])
        for ext, count in ext_counter.most_common():
            w.writerow([ext, count, round(count/total_files*100, 1),
                       format_size(ext_sizes[ext]), format_size(ext_sizes[ext]/count)])

    # RAS file counts
    with open(f"{OUTPUT_DIR}/blob_ras_coverage.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ras_id", "num_files", "total_size", "file_types"])
        for ras_id in sorted(ras_files.keys(), key=lambda x: len(ras_files[x]), reverse=True)[:500]:
            files = ras_files[ras_id]
            exts = set(fi["ext"] for fi in files)
            total = sum(fi["size"] for fi in files)
            w.writerow([ras_id, len(files), format_size(total), ", ".join(sorted(exts))])

    # Sample paths
    with open(f"{OUTPUT_DIR}/blob_sample_paths.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["path", "size", "extension"])
        for s in sample_paths:
            w.writerow([s["path"], s["size"], s["ext"]])

    print(f"\n[DONE] Output saved to:")
    print(f"  {OUTPUT_DIR}/blob_file_types.csv")
    print(f"  {OUTPUT_DIR}/blob_ras_coverage.csv")
    print(f"  {OUTPUT_DIR}/blob_sample_paths.csv")


if __name__ == "__main__":
    main()
