"""
Loads all dimension CSVs + fait_accident.csv into PostgreSQL.
Run this AFTER build_fait.py has generated fait_accident.csv
and AFTER schema_accidents.sql has been run to create the tables.
"""

import pandas as pd
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────────────────────
# CONFIGURATION 
# ─────────────────────────────────────────────────────────────
DB_URL = "postgresql://motdepasse@localhost:5432/accidents_db"
DATA_DIR = "./data/dims"
CHUNKSIZE = 50_000

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
    if table in RENAME_MAP:
        chunk = chunk.rename(columns=RENAME_MAP[table])
    for col in DROP_COLS.get(table, []):
        chunk = chunk.drop(columns=[col], errors="ignore")
    if table == "fait_accident":
        chunk = chunk.drop(columns=["id_meteo"], errors="ignore")
    if table == "dim_date" and "heure" not in chunk.columns:
        chunk["heure"] = None
    for col in BOOL_COLS.get(table, []):
        if col in chunk.columns:
            chunk[col] = chunk[col].astype(float).astype("boolean")
    for col in STR_COLS.get(table, []):
        if col in chunk.columns:
            chunk[col] = chunk[col].astype(str).replace({"nan": None, "None": None})
    return chunk

def detect_params(filepath):
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
    # Récupérer les colonnes
    cols = list(chunk.columns)
    col_str = ", ".join(cols)
    placeholders = ", ".join([f":{c}" for c in cols])
    
    sql = text(f"""
        INSERT INTO {table} ({col_str})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING  -- align with our updating data mart policy
    """)
    
    with engine.begin() as conn:
        conn.execute(sql, chunk.to_dict(orient="records"))
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

# ─────────────────────────────────────────────────────────────
# MAIN — dimensions d'abord, fait_accident en dernier
# ─────────────────────────────────────────────────────────────

def main():
    print("Connecting to database...")
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✓ Connected\n")

    load_simple("dim_pays.csv",         "dim_pays",         engine)
    load_simple("dim_date.csv",         "dim_date",         engine)
    load_simple("dim_meteo.csv",        "dim_meteo",        engine)
    load_simple("dim_localisation.csv", "dim_localisation", engine)
    load_simple("dim_usager.csv",       "dim_usager",       engine)
    load_simple("dim_vehicule.csv",     "dim_vehicule",     engine)
    load_simple("fait_accident.csv",    "fait_accident",    engine)

    print(f"\n{'═'*48}")
    print(" Load complete")

if __name__ == "__main__":
    main()