"""
load_db.py
==========
Loads all dimension CSVs + fait_accident.csv into PostgreSQL.
Run this AFTER build_fait.py has generated fait_accident.csv
and AFTER schema_accidents.sql has been run to create the tables.

Usage:
  python load_db.py
"""

import pandas as pd
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────────────────────
# CONFIGURATION  ← edit these
# ─────────────────────────────────────────────────────────────
DB_URL   = "postgresql://postgres:mypassword@localhost:5432/accidents_db"
DATA_DIR = "./data/dims"   # output folder from build_dims_and_fait.py
CHUNKSIZE= 50_000

# ─────────────────────────────────────────────────────────────
# FILES TO LOAD  (filename, table, id_col for FK offset if paired)
# ─────────────────────────────────────────────────────────────
# All files are now in a single folder — no FR/UK split needed
# IDs are already consistent across countries (built by build_dims_and_fait.py)
SIMPLE_FILES = [
    ("dim_pays.csv",        "dim_pays",        None),
    ("dim_date.csv",        "dim_date",        None),
    ("dim_meteo.csv",       "dim_meteo",       None),
    ("dim_localisation.csv","dim_localisation", None),
    ("dim_usager.csv",      "dim_usager",      None),
    ("dim_vehicule.csv",    "dim_vehicule",    None),
    ("fait_accident.csv",   "fait_accident",   None),
]

PAIRED_FILES = []   # no longer needed — IDs unified during build step

RENAME_MAP = {
    "dim_meteo": {"T_min": "temp_min", "T_max": "temp_max"},
}

DROP_COLS = {
    "dim_date": ["est_ferie_uk"],
}

BOOL_COLS = {
    "dim_date": ["est_weekend", "est_jour_ferie"],
}

STR_COLS = {
    "dim_localisation": ["commune", "departement", "district"],
}

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def clean(chunk):
    for col in chunk.select_dtypes(include="object").columns:
        chunk[col] = (chunk[col].astype(str)
                                .str.replace("\xa0", "", regex=False)
                                .str.strip()
                                .replace({"nan": None, "None": None, "N/A": None}))
    return chunk

def transform(chunk, table):
    # Renames
    if table in RENAME_MAP:
        chunk = chunk.rename(columns=RENAME_MAP[table])
    # Drop columns
    for col in DROP_COLS.get(table, []):
        chunk = chunk.drop(columns=[col], errors="ignore")
    # Drop id_meteo from fait_accident — no longer in schema
    if table == "fait_accident":
        chunk = chunk.drop(columns=["id_meteo"], errors="ignore")
    # Add missing heure
    if table == "dim_date" and "heure" not in chunk.columns:
        chunk["heure"] = None
    # Boolean casting
    for col in BOOL_COLS.get(table, []):
        if col in chunk.columns:
            chunk[col] = chunk[col].astype(float).astype("boolean")
    # String casting (for codes like "2B033")
    for col in STR_COLS.get(table, []):
        if col in chunk.columns:
            chunk[col] = chunk[col].astype(str).replace({"nan": None, "None": None})
    return chunk

def detect_params(filepath):
    """Detect encoding and separator using csv.Sniffer on a larger sample."""
    import csv
    encodings = ("utf-8", "latin-1", "utf-8-sig")
    for encoding in encodings:
        try:
            with open(filepath, "r", encoding=encoding) as f:
                sample = f.read(8192)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=",;\t ")
            return encoding, dialect.delimiter
        except UnicodeDecodeError:
            continue
        except csv.Error:
            for sep in (",", ";", "\t", " "):
                try:
                    sample_df = pd.read_csv(filepath, nrows=5,
                                            encoding=encoding, sep=sep,
                                            low_memory=False)
                    if len(sample_df.columns) > 1:
                        return encoding, sep
                except Exception:
                    continue
    return "latin-1", ","

def read_chunks(filepath):
    encoding, sep = detect_params(filepath)
    for chunk in pd.read_csv(filepath, chunksize=CHUNKSIZE,
                              low_memory=False, encoding=encoding,
                              sep=sep, on_bad_lines="skip"):
        yield chunk

def stream(filepath, table):
    for chunk in read_chunks(filepath):
        yield transform(clean(chunk), table)

def write(chunk, table, engine):
    chunk.to_sql(table, engine, if_exists="append", index=False)

# ─────────────────────────────────────────────────────────────
# LOADERS
# ─────────────────────────────────────────────────────────────

def load_simple(filename, table, engine):
    print(f"\n── {table.upper()}")
    total = 0
    for chunk in stream(f"{DATA_DIR}/{filename}", table):
        write(chunk, table, engine)
        total += len(chunk)
    print(f"  ✓ {total:,} rows → {table}")

def load_paired(fr_file, uk_file, table, id_col, engine):
    print(f"\n── {table.upper()}")
    fr_max = 0
    total_fr = 0

    for chunk in stream(f"{DATA_DIR}/{fr_file}", table):
        fr_max    = max(fr_max, int(chunk[id_col].max()))
        total_fr += len(chunk)
        write(chunk, table, engine)
    print(f"    FR: {total_fr:,} rows  (max {id_col} = {fr_max})")

    total_uk = 0
    for chunk in stream(f"{DATA_DIR}/{uk_file}", table):
        chunk[id_col] = chunk[id_col] + fr_max
        total_uk     += len(chunk)
        write(chunk, table, engine)
    print(f"    UK: {total_uk:,} rows  (offset +{fr_max})")
    print(f"  ✓ {total_fr + total_uk:,} total rows → {table}")

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("Connecting to database...")
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✓ Connected\n")

    # Load fait_accident last
    load_simple("fait_accident.csv", "fait_accident", engine)

    print(f"\n{'═'*48}")
    print("✓ Load complete")

if __name__ == "__main__":
    main()