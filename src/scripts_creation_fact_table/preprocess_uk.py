"""
preprocess_uk.py
================
Filters all UK raw files to keep only years matching FR data:
2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021

Processes files in chunks to handle large 3GB+ files.
Output files are written to RAW_DIR/accidents_uk_filtered/

Run this BEFORE build_fait.py and load_db.py.
Then update RAW_DIR in those scripts to point to the filtered folder.

Files processed:
  accidents_uk/collision.csv  → filtered on collision_year column
  accidents_uk/casualty.csv   → filtered on collision_year column
  accidents_uk/vehicle.csv    → filtered on collision_year column

Usage:
  python preprocess_uk.py
"""

import os
import csv as csv_mod
import pandas as pd

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
RAW_DIR    = "./raw"
OUT_DIR    = "./raw/accidents_uk_filtered"
CHUNKSIZE  = 50_000
KEEP_YEARS = {2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021}

# UK files to filter and the column that contains the year
UK_FILES = [
    ("collision.csv", "collision_year"),
    ("casualty.csv",  "collision_year"),
    ("vehicle.csv",   "collision_year"),
]

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def detect_params(filepath):
    """Detect encoding and separator using csv.Sniffer."""
    encodings = ("utf-8", "latin-1", "utf-8-sig")
    for encoding in encodings:
        try:
            with open(filepath, "r", encoding=encoding) as f:
                sample = f.read(8192)
            sniffer = csv_mod.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=",;\t ")
            return encoding, dialect.delimiter
        except UnicodeDecodeError:
            continue
        except csv_mod.Error:
            for sep in (",", ";", "\t", " "):
                try:
                    df = pd.read_csv(filepath, nrows=5,
                                     encoding=encoding, sep=sep,
                                     low_memory=False)
                    if len(df.columns) > 1:
                        return encoding, sep
                except Exception:
                    continue
    return "latin-1", ","

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def filter_file(filename, year_col):
    in_path  = os.path.join(RAW_DIR, "accidents_uk", filename)
    out_path = os.path.join(OUT_DIR, filename)

    print(f"\n── {filename}")
    print(f"   Detecting format...", end=" ", flush=True)
    encoding, sep = detect_params(in_path)
    print(f"encoding={encoding}  sep={repr(sep)}")

    total_read    = 0
    total_written = 0
    header_written = False

    for chunk in pd.read_csv(in_path, chunksize=CHUNKSIZE, low_memory=False,
                              encoding=encoding, sep=sep, on_bad_lines="skip"):
        total_read += len(chunk)

        # Filter to keep only matching years
        if year_col in chunk.columns:
            chunk = chunk[chunk[year_col].isin(KEEP_YEARS)]
        else:
            print(f"   ⚠ Column '{year_col}' not found — keeping all rows")

        if len(chunk) == 0:
            continue

        # Write header only once, then append
        chunk.to_csv(out_path,
                     mode="w" if not header_written else "a",
                     header=not header_written,
                     index=False,
                     encoding="utf-8")
        header_written  = True
        total_written  += len(chunk)
        print(f"   ... {total_read:,} read  {total_written:,} kept", end="\r")

    pct = round(total_written / total_read * 100, 1) if total_read else 0
    print(f"   ✓ {total_read:,} rows read  →  {total_written:,} kept ({pct}%)")
    print(f"   Saved to: {out_path}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    print("=" * 52)
    print("UK Data Preprocessor")
    print(f"Keeping years: {sorted(KEEP_YEARS)}")
    print("=" * 52)

    for filename, year_col in UK_FILES:
        filter_file(filename, year_col)

    print(f"\n{'='*52}")
    print("✓ Preprocessing complete")
    print(f"  Filtered files saved to: {OUT_DIR}")
    print()
    print("Next steps:")
    print("  1. Update UK file paths in build_fait.py and load_db.py")
    print(f"     to point to: {OUT_DIR}")
    print("  2. Run: python build_fait.py")
    print("  3. Run: python load_db.py")

if __name__ == "__main__":
    main()