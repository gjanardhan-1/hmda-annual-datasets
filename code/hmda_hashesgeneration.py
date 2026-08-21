#!/usr/bin/env python
# coding: utf-8

# In[2]:


"""
hmda_manifest.py

Build a download manifest + SHA-256 checksum file for a folder of HMDA
LAR / TS ZIP archives downloaded from CFPB/FFIEC.

Usage:
    python hmda_manifest.py "C:\\path\\to\\hmda_zips"
    (or just run "python hmda_manifest.py" and you'll be prompted for the folder)

Outputs (written into the SAME folder as the ZIP files):
    hmda_download_manifest.csv
    SHA256SUMS.txt
    hash_errors.log   (only created if at least one file fails)

Only standard library is used, except for tqdm (progress bar).
Install tqdm if needed:  pip install tqdm
"""

import sys
import os
import csv
import hashlib
import platform
from pathlib import Path
from datetime import datetime

try:
    from tqdm import tqdm
except ImportError:
    print("The 'tqdm' package is required for the progress bar.")
    print("Install it with:  pip install tqdm")
    sys.exit(1)


# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB read chunks while hashing (keeps memory low)
MANIFEST_FILENAME = "hmda_download_manifest.csv"
CHECKSUMS_FILENAME = "SHA256SUMS.txt"
ERROR_LOG_FILENAME = "hash_errors.log"


# ----------------------------------------------------------------------
# STEP 1: Get the target folder from the user
#   - Priority: command-line argument
#   - Fallback: interactively prompt the user
# ----------------------------------------------------------------------
def get_target_folder():
    if len(sys.argv) >= 2:
        folder = sys.argv[1]
    else:
        folder = input("I:\HMDA: ").strip()

    # Strip surrounding quotes if the user pasted a quoted Windows path
    folder = folder.strip('"').strip("'")

    folder_path = Path(folder)
    if not folder_path.is_dir():
        print(f"ERROR: '{folder_path}' is not a valid directory.")
        sys.exit(1)

    return folder_path


# ----------------------------------------------------------------------
# STEP 2: Find every .zip file in that folder (non-recursive, top level only)
# ----------------------------------------------------------------------
def find_zip_files(folder_path):
    zip_files = sorted(folder_path.glob("*.zip"))
    if not zip_files:
        print(f"No .zip files found in '{folder_path}'.")
        sys.exit(1)
    return zip_files


# ----------------------------------------------------------------------
# STEP 3: Verify a file is readable before hashing
# ----------------------------------------------------------------------
def is_file_readable(file_path):
    try:
        with open(file_path, "rb") as f:
            f.read(1)  # try reading a single byte
        return True
    except (OSError, IOError):
        return False


# ----------------------------------------------------------------------
# STEP 4: Compute SHA-256 checksum, streaming the file in chunks so large
#         files (multi-GB ZIPs) never need to be fully loaded into memory.
#         A per-file progress bar (in MB) is shown via tqdm.
# ----------------------------------------------------------------------
def compute_sha256(file_path, file_size_bytes):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        with tqdm(
            total=file_size_bytes,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Hashing {file_path.name}",
            leave=False,
        ) as bar:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                sha256.update(chunk)
                bar.update(len(chunk))
    return sha256.hexdigest()


# ----------------------------------------------------------------------
# STEP 5: Get file creation / modified timestamps.
#   NOTE: True "creation time" is only reliably available on Windows via
#   os.path.getctime(). On Linux/Mac, getctime() actually returns the
#   last metadata-change time, not creation time (true creation time is
#   often unavailable on those platforms). Since this script is intended
#   to run on Windows, getctime() will correctly reflect creation time
#   there. We label it clearly and fall back gracefully if unavailable.
# ----------------------------------------------------------------------
def get_timestamps(file_path):
    stat_result = file_path.stat()

    try:
        created = datetime.fromtimestamp(stat_result.st_ctime)
        created_str = created.strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, ValueError):
        created_str = "N/A"

    try:
        modified = datetime.fromtimestamp(stat_result.st_mtime)
        modified_str = modified.strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, ValueError):
        modified_str = "N/A"

    return created_str, modified_str


# ----------------------------------------------------------------------
# STEP 6: Process all ZIP files, collecting manifest rows and checksum
#         lines, and logging any errors encountered along the way.
# ----------------------------------------------------------------------
def process_files(zip_files):
    manifest_rows = []
    checksum_lines = []
    error_lines = []

    success_count = 0
    fail_count = 0

    print(f"\nFound {len(zip_files)} ZIP file(s). Starting processing...\n")

    # Outer progress bar across all files
    for file_path in tqdm(zip_files, desc="Overall progress", unit="file"):
        try:
            # --- readability check ---
            if not is_file_readable(file_path):
                raise IOError(f"File is not readable (possibly corrupted or locked): {file_path.name}")

            # --- basic file stats ---
            file_size_bytes = file_path.stat().st_size
            file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
            created_str, modified_str = get_timestamps(file_path)

            # --- hash ---
            sha256_hash = compute_sha256(file_path, file_size_bytes)

            # --- record success ---
            manifest_rows.append({
                "File Name": file_path.name,
                "File Size (Bytes)": file_size_bytes,
                "File Size (MB)": file_size_mb,
                "SHA-256": sha256_hash,
                "Created": created_str,
                "Modified": modified_str,
            })
            checksum_lines.append(f"{sha256_hash}  {file_path.name}")
            success_count += 1

        except Exception as exc:
            # Log the error but keep processing remaining files
            fail_count += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_lines.append(f"[{timestamp}] {file_path.name}: {exc}")

    return manifest_rows, checksum_lines, error_lines, success_count, fail_count


# ----------------------------------------------------------------------
# STEP 7: Write outputs -- manifest CSV, SHA256SUMS.txt, and error log
#         (error log only written if there were failures)
# ----------------------------------------------------------------------
def write_manifest_csv(folder_path, manifest_rows):
    manifest_path = folder_path / MANIFEST_FILENAME
    fieldnames = ["File Name", "File Size (Bytes)", "File Size (MB)", "SHA-256", "Created", "Modified"]

    with open(manifest_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(manifest_rows)

    return manifest_path


def write_checksums_txt(folder_path, checksum_lines):
    checksums_path = folder_path / CHECKSUMS_FILENAME
    with open(checksums_path, "w", encoding="utf-8") as f:
        f.write("\n".join(checksum_lines) + "\n")
    return checksums_path


def write_error_log(folder_path, error_lines):
    if not error_lines:
        return None
    error_log_path = folder_path / ERROR_LOG_FILENAME
    with open(error_log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(error_lines) + "\n")
    return error_log_path


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
def main():
    folder_path = get_target_folder()
    zip_files = find_zip_files(folder_path)

    manifest_rows, checksum_lines, error_lines, success_count, fail_count = process_files(zip_files)

    manifest_path = write_manifest_csv(folder_path, manifest_rows)
    checksums_path = write_checksums_txt(folder_path, checksum_lines)
    error_log_path = write_error_log(folder_path, error_lines)

    # --- final summary ---
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Files processed : {len(zip_files)}")
    print(f"Successful      : {success_count}")
    print(f"Failed          : {fail_count}")
    print(f"Manifest saved  : {manifest_path}")
    print(f"Checksums saved : {checksums_path}")
    if error_log_path:
        print(f"Errors logged   : {error_log_path}")


if __name__ == "__main__":
    main()


# In[4]:


"""
hmda_manifest.py

Build a download manifest + SHA-256 checksum file for a folder of HMDA
LAR / TS ZIP archives downloaded from CFPB/FFIEC.

Usage:
    python hmda_manifest.py "C:\\path\\to\\hmda_zips"
    (or just run "python hmda_manifest.py" and you'll be prompted for the folder)

Outputs (written into the SAME folder as the ZIP files):
    hmda_download_manifest.csv
    SHA256SUMS.txt
    hash_errors.log   (only created if at least one file fails)

Only standard library is used, except for tqdm (progress bar).
Install tqdm if needed:  pip install tqdm
"""

import sys
import os
import csv
import hashlib
import platform
from pathlib import Path
from datetime import datetime

try:
    from tqdm import tqdm
except ImportError:
    print("The 'tqdm' package is required for the progress bar.")
    print("Install it with:  pip install tqdm")
    sys.exit(1)


# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB read chunks while hashing (keeps memory low)
MANIFEST_FILENAME = "hmda_download_manifest.csv"
CHECKSUMS_FILENAME = "SHA256SUMS.txt"
ERROR_LOG_FILENAME = "hash_errors.log"


# ----------------------------------------------------------------------
# STEP 1: Get the target folder from the user
#   - Priority: command-line argument
#   - Fallback: interactively prompt the user
# ----------------------------------------------------------------------
def get_target_folder():
    # When running inside Jupyter/IPython, sys.argv is NOT your real
    # command-line arguments -- it contains the kernel's own launch
    # flags, e.g.: ['ipykernel_launcher.py', '-f', 'C:\\...\\kernel-xxx.json']
    # We detect that pattern and ignore it, falling back to the input()
    # prompt instead. A real folder path won't start with '-' and won't
    # end in '.json', so we filter on that.
    candidate = None
    if len(sys.argv) >= 2:
        arg = sys.argv[1]
        if not arg.startswith("-") and not arg.lower().endswith(".json"):
            candidate = arg

    if candidate:
        folder = candidate
    else:
        folder = input("I:\HMDA: ").strip()

    # Strip surrounding quotes if the user pasted a quoted Windows path
    folder = folder.strip('"').strip("'")

    folder_path = Path(folder)
    if not folder_path.is_dir():
        print(f"ERROR: '{folder_path}' is not a valid directory.")
        sys.exit(1)

    return folder_path


# ----------------------------------------------------------------------
# STEP 2: Find every .zip file in that folder (non-recursive, top level only)
# ----------------------------------------------------------------------
def find_zip_files(folder_path):
    zip_files = sorted(folder_path.glob("*.zip"))
    if not zip_files:
        print(f"No .zip files found in '{folder_path}'.")
        sys.exit(1)
    return zip_files


# ----------------------------------------------------------------------
# STEP 3: Verify a file is readable before hashing
# ----------------------------------------------------------------------
def is_file_readable(file_path):
    try:
        with open(file_path, "rb") as f:
            f.read(1)  # try reading a single byte
        return True
    except (OSError, IOError):
        return False


# ----------------------------------------------------------------------
# STEP 4: Compute SHA-256 checksum, streaming the file in chunks so large
#         files (multi-GB ZIPs) never need to be fully loaded into memory.
#         A per-file progress bar (in MB) is shown via tqdm.
# ----------------------------------------------------------------------
def compute_sha256(file_path, file_size_bytes):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        with tqdm(
            total=file_size_bytes,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Hashing {file_path.name}",
            leave=False,
        ) as bar:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                sha256.update(chunk)
                bar.update(len(chunk))
    return sha256.hexdigest()


# ----------------------------------------------------------------------
# STEP 5: Get file creation / modified timestamps.
#   NOTE: True "creation time" is only reliably available on Windows via
#   os.path.getctime(). On Linux/Mac, getctime() actually returns the
#   last metadata-change time, not creation time (true creation time is
#   often unavailable on those platforms). Since this script is intended
#   to run on Windows, getctime() will correctly reflect creation time
#   there. We label it clearly and fall back gracefully if unavailable.
# ----------------------------------------------------------------------
def get_timestamps(file_path):
    stat_result = file_path.stat()

    try:
        created = datetime.fromtimestamp(stat_result.st_ctime)
        created_str = created.strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, ValueError):
        created_str = "N/A"

    try:
        modified = datetime.fromtimestamp(stat_result.st_mtime)
        modified_str = modified.strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, ValueError):
        modified_str = "N/A"

    return created_str, modified_str


# ----------------------------------------------------------------------
# STEP 6: Process all ZIP files, collecting manifest rows and checksum
#         lines, and logging any errors encountered along the way.
# ----------------------------------------------------------------------
def process_files(zip_files):
    manifest_rows = []
    checksum_lines = []
    error_lines = []

    success_count = 0
    fail_count = 0

    print(f"\nFound {len(zip_files)} ZIP file(s). Starting processing...\n")

    # Outer progress bar across all files
    for file_path in tqdm(zip_files, desc="Overall progress", unit="file"):
        try:
            # --- readability check ---
            if not is_file_readable(file_path):
                raise IOError(f"File is not readable (possibly corrupted or locked): {file_path.name}")

            # --- basic file stats ---
            file_size_bytes = file_path.stat().st_size
            file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
            created_str, modified_str = get_timestamps(file_path)

            # --- hash ---
            sha256_hash = compute_sha256(file_path, file_size_bytes)

            # --- record success ---
            manifest_rows.append({
                "File Name": file_path.name,
                "File Size (Bytes)": file_size_bytes,
                "File Size (MB)": file_size_mb,
                "SHA-256": sha256_hash,
                "Created": created_str,
                "Modified": modified_str,
            })
            checksum_lines.append(f"{sha256_hash}  {file_path.name}")
            success_count += 1

        except Exception as exc:
            # Log the error but keep processing remaining files
            fail_count += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_lines.append(f"[{timestamp}] {file_path.name}: {exc}")

    return manifest_rows, checksum_lines, error_lines, success_count, fail_count


# ----------------------------------------------------------------------
# STEP 7: Write outputs -- manifest CSV, SHA256SUMS.txt, and error log
#         (error log only written if there were failures)
# ----------------------------------------------------------------------
def write_manifest_csv(folder_path, manifest_rows):
    manifest_path = folder_path / MANIFEST_FILENAME
    fieldnames = ["File Name", "File Size (Bytes)", "File Size (MB)", "SHA-256", "Created", "Modified"]

    with open(manifest_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(manifest_rows)

    return manifest_path


def write_checksums_txt(folder_path, checksum_lines):
    checksums_path = folder_path / CHECKSUMS_FILENAME
    with open(checksums_path, "w", encoding="utf-8") as f:
        f.write("\n".join(checksum_lines) + "\n")
    return checksums_path


def write_error_log(folder_path, error_lines):
    if not error_lines:
        return None
    error_log_path = folder_path / ERROR_LOG_FILENAME
    with open(error_log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(error_lines) + "\n")
    return error_log_path


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
def main():
    folder_path = get_target_folder()
    zip_files = find_zip_files(folder_path)

    manifest_rows, checksum_lines, error_lines, success_count, fail_count = process_files(zip_files)

    manifest_path = write_manifest_csv(folder_path, manifest_rows)
    checksums_path = write_checksums_txt(folder_path, checksum_lines)
    error_log_path = write_error_log(folder_path, error_lines)

    # --- final summary ---
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Files processed : {len(zip_files)}")
    print(f"Successful      : {success_count}")
    print(f"Failed          : {fail_count}")
    print(f"Manifest saved  : {manifest_path}")
    print(f"Checksums saved : {checksums_path}")
    if error_log_path:
        print(f"Errors logged   : {error_log_path}")


if __name__ == "__main__":
    main()

