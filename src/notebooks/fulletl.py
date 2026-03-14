"""
ETL Script - Road Accidents Database (final v2 — chunked)
==========================================================
Loads 9 dimension CSV files into PostgreSQL in chunks to avoid
running out of memory on large files.

Files expected in DATA_DIR:
  dim_usagers_fr.csv        dim_usagers_uk.csv
  dim_localisation_fr.csv   dim_localisation_uk.csv
  dim_vehicule_fr.csv       vehicule_uk.csv
  dim_meteo.csv
  DIM_PAYS.csv
  DIM_TEMPS.csv

Usage:
  pip install pandas sqlalchemy psycopg2-binary
  python etl_final.py
"""

import pandas as pd
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────────────────────
# CONFIGURATION  
# ─────────────────────────────────────────────────────────────
DB_URL    = "postgresql://postgres:mypassword@localhost:5432/accidents_db"
DATA_DIR  = "./data/processed" 
CHUNKSIZE = 50_000  

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def path(filename):
    return f"{DATA_DIR}/{filename}"

def clean_chunk(chunk):
    for col in chunk.select_dtypes(include="object").columns:
        chunk[col] = (chunk[col].astype(str)
                                .str.replace("\xa0", "", regex=False)
                                .str.strip()
                                .replace({"nan": None, "None": None, "N/A": None}))
    return chunk

def read_full(filename):
    df = pd.read_csv(path(filename), low_memory=False)
    return clean_chunk(df)

def stream(filename):
    for chunk in pd.read_csv(path(filename), chunksize=CHUNKSIZE, low_memory=False):
        yield clean_chunk(chunk)

def append_chunk(chunk, keep_cols, table, engine):
    cols = [c for c in keep_cols if c in chunk.columns]
    chunk[cols].to_sql(table, engine, if_exists="append", index=False)


def load_full(df, table, engine):
    df.to_sql(table, engine, if_exists="append", index=False)
    print(f"  ✓ {len(df):,} rows → {table}")

# ─────────────────────────────────────────────────────────────
# NORMALIZATION MAPS  (FR raw values → unified English)
# ─────────────────────────────────────────────────────────────

SEXE_MAP = {
    "Masculin": "Male", "Féminin": "Female", "Inconnu": "Unknown",
    "Male": "Male", "Female": "Female", "Unknown": "Unknown",
}

GRAVITE_MAP = {
    "Tué":                  "Killed",
    "Blessé hospitalisé":   "Seriously injured",
    "Blessé léger":         "Slightly injured",
    "Indemne":              "Uninjured",
    "Killed":               "Killed",
    "Seriously injured":    "Seriously injured",
    "Slightly injured":     "Slightly injured",
    "Uninjured":            "Uninjured",
    "Slight":               "Slightly injured",
    "Serious":              "Seriously injured",
    "Fatal":                "Killed",
}

CAT_USAGER_MAP = {
    "Conducteur":           "Driver",
    "Passager":             "Passenger",
    "Piéton":               "Pedestrian",
    "Driver":               "Driver",
    "Driver or rider":      "Driver",
    "Passenger":            "Passenger",
    "Pedestrian":           "Pedestrian",
}

PLACE_VEHICULE_MAP = {
    "Avant gauche":         "Front left",
    "Avant droit":          "Front right",
    "Avant centre":         "Front centre",
    "Arrière gauche":       "Rear left",
    "Arrière droit":        "Rear right",
    "Arrière centre":       "Rear centre",
    "Arrière":              "Rear",
    "Unknown":              "Unknown",
    "Not passenger":        "Not passenger",
    "Front seat":           "Front right",
}

TYPE_VEHICULE_MAP = {
    "VL seul":                      "Car",
    "PL seul":                      "Heavy goods vehicle",
    "Deux-roues motorisé":          "Motorcycle",
    "Vélo":                         "Bicycle",
    "Piéton":                       "Pedestrian",
    "Véhicule utilitaire":          "Van",
    "Autobus":                      "Bus",
    "Autocar":                      "Coach",
    "Tracteur routier":             "Heavy goods vehicle",
    "Engin spécial":                "Special vehicle",
    "Tramway":                      "Tram",
    "Train":                        "Train",
    "Autre":                        "Other",
    "Indéterminable":               "Unknown",
}

MANOEUVRE_MAP = {
    "Sans changement de direction": "Going ahead",
    "En faisant demi-tour":         "U-turn",
    "Tournant à droite":            "Turning right",
    "Tournant à gauche":            "Turning left",
    "En dépassant":                 "Overtaking",
    "En changeant de file":         "Changing lane",
    "A l'arrêt":                   "Stopped",
    "Stationnement en manœuvre":    "Parking",
    "Marche arrière":               "Reversing",
    "Franchissement de TPC":        "Crossing median",
    "Evitement d'obstacle":        "Avoiding obstacle",
    "Ouverture de porte":           "Door opening",
    "Going ahead":                  "Going ahead",
    "Turning right":                "Turning right",
    "Turning left":                 "Turning left",
    "U-turn":                       "U-turn",
    "Reversing":                    "Reversing",
    "Overtaking":                   "Overtaking",
    "Waiting to go ahead":          "Waiting to go ahead",
    "Parked":                       "Parked",
}

MOTORISATION_MAP = {
    "Essence":      "Petrol",
    "Diesel":       "Diesel",
    "Hybride":      "Hybrid",
    "Électrique":   "Electric",
    "Inconnu":      "Unknown",
    "Unknown":      "Unknown",
    "Hydrogène":    "Hydrogen",
    "Autre":        "Other",
    "Petrol":       "Petrol",
    "Heavy oil":    "Diesel",
    "Electric":     "Electric",
    "Hybrid":       "Hybrid",
}

TYPE_ROUTE_MAP = {
    "Autoroute":                "Motorway",
    "Route nationale":          "National road",
    "Route départementale":     "Departmental road",
    "Voie communale":           "Municipal road",
    "Chemin rural":             "Rural road",
    "Voie privée":              "Private road",
    "Motorway":                 "Motorway",
    "A road":                   "A road",
    "B road":                   "B road",
    "C road":                   "C road",
    "Unclassified":             "Unclassified",
}

def normalize_col(series, mapping):
    """Map values through a normalization dict, keeping unmapped values as-is."""
    return series.map(lambda x: mapping.get(str(x), x) if x is not None else None)

# ─────────────────────────────────────────────────────────────
# CHUNKED FR+UK MERGE LOADER
# ─────────────────────────────────────────────────────────────

def load_fr_uk_chunked(fr_file, uk_file, id_col, keep_cols, table, engine):
    print(f"\n── {table.upper()}")
    total_fr = 0
    fr_max   = 0

    for chunk in stream(fr_file):
        fr_max    = max(fr_max, int(chunk[id_col].max()))
        total_fr += len(chunk)
        append_chunk(chunk, keep_cols, table, engine)
    print(f"    FR: {total_fr:,} rows  (max {id_col} = {fr_max})")

    total_uk = 0
    for chunk in stream(uk_file):
        chunk[id_col] = chunk[id_col] + fr_max
        total_uk     += len(chunk)
        append_chunk(chunk, keep_cols, table, engine)
    print(f"    UK: {total_uk:,} rows  (offset +{fr_max})")
    print(f"  ✓ {total_fr + total_uk:,} total rows → {table}")

    return fr_max


# ─────────────────────────────────────────────────────────────
# 1. DIM_PAYS
# ─────────────────────────────────────────────────────────────

def load_dim_pays(engine):
    print("\n── DIM_PAYS")
    df = read_full("DIM_PAYS.csv")
    load_full(df, "dim_pays", engine)


# ─────────────────────────────────────────────────────────────
# 2. DIM_DATE
# ─────────────────────────────────────────────────────────────

def load_dim_date(engine):
    print("\n── DIM_DATE")
    total = 0
    for chunk in stream("DIM_TEMPS.csv"):
        if "id_temps" in chunk.columns and "id_date" not in chunk.columns:
            chunk = chunk.rename(columns={"id_temps": "id_date"})
        if "est_ferie_fr" in chunk.columns and "est_jour_ferie" not in chunk.columns:
            chunk = chunk.rename(columns={"est_ferie_fr": "est_jour_ferie"})
        chunk = chunk.drop(columns=["est_ferie_uk"], errors="ignore")
        if "heure" not in chunk.columns:
            chunk["heure"] = None
        # Cast 0/1 integers to proper Python booleans for PostgreSQL
        for col in ["est_weekend", "est_jour_ferie"]:
            if col in chunk.columns:
                chunk[col] = chunk[col].astype(float).astype("boolean")
        keep = ["id_date", "date", "annee", "mois", "jour", "heure",
                "saison", "est_weekend", "est_jour_ferie"]
        append_chunk(chunk, keep, "dim_date", engine)
        total += len(chunk)
    print(f"  ✓ {total:,} rows → dim_date")


# ─────────────────────────────────────────────────────────────
# 3. DIM_METEO
# ─────────────────────────────────────────────────────────────

def load_dim_meteo(engine):
    print("\n── DIM_METEO")
    total = 0
    for chunk in stream("dim_meteo.csv"):
        chunk = chunk.rename(columns={"T_min": "temp_min", "T_max": "temp_max"})
        keep  = ["id_meteo", "id_pays", "date", "temp_min", "temp_max",
                 "precipitations", "vent", "conditions"]
        append_chunk(chunk, keep, "dim_meteo", engine)
        total += len(chunk)
    print(f"  ✓ {total:,} rows → dim_meteo")


# ─────────────────────────────────────────────────────────────
# 4. DIM_LOCALISATION
# ─────────────────────────────────────────────────────────────

def convert_lambert93(chunk):
    """
    Convert Lambert-93 (EPSG:2154) coordinates to WGS84 (EPSG:4326)
    for French rows where lat/lon look like Lambert values (> 1000).
    """
    from pyproj import Transformer
    transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
    mask = (
        chunk["id_pays"] == 1 &
        chunk["latitude"].notna() &
        (chunk["latitude"].abs() > 1000)   # Lambert coords are in the millions
    )
    if mask.any():
        lons, lats = transformer.transform(
            chunk.loc[mask, "longitude"].values,
            chunk.loc[mask, "latitude"].values
        )
        chunk.loc[mask, "latitude"]  = lons.round(6)
        chunk.loc[mask, "longitude"] = lats.round(6)
    return chunk

def load_dim_localisation(engine):
    print("\n── DIM_LOCALISATION")
    keep     = ["id_lieu", "id_pays", "commune", "departement", "district",
                "type_route", "vitesse_limite", "latitude", "longitude"]
    total_fr = 0
    fr_max   = 0

    # FR — with Lambert→WGS84 conversion
    for chunk in stream("dim_localisation_fr.csv"):
        # Cast commune/departement to string to handle codes like "2B033"
        for col in ["commune", "departement", "district"]:
            if col in chunk.columns:
                chunk[col] = chunk[col].astype(str).replace({"nan": None, "None": None})
        chunk    = convert_lambert93(chunk)
        if "type_route" in chunk.columns:
            chunk["type_route"] = normalize_col(chunk["type_route"], TYPE_ROUTE_MAP)
        fr_max   = max(fr_max, int(chunk["id_lieu"].max()))
        total_fr+= len(chunk)
        append_chunk(chunk, keep, "dim_localisation", engine)
    print(f"    FR: {total_fr:,} rows  (max id_lieu = {fr_max})")

    # UK — no coordinate conversion needed
    total_uk = 0
    for chunk in stream("dim_localisation_uk.csv"):
        for col in ["commune", "departement", "district"]:
            if col in chunk.columns:
                chunk[col] = chunk[col].astype(str).replace({"nan": None, "None": None})
        chunk["id_lieu"] = chunk["id_lieu"] + fr_max
        total_uk        += len(chunk)
        append_chunk(chunk, keep, "dim_localisation", engine)
    print(f"    UK: {total_uk:,} rows  (offset +{fr_max})")
    print(f"  ✓ {total_fr + total_uk:,} total rows → dim_localisation")
    return fr_max


# ─────────────────────────────────────────────────────────────
# 5. DIM_USAGER
# ─────────────────────────────────────────────────────────────

def normalize_usager(chunk):
    if "sexe"           in chunk.columns: chunk["sexe"]           = normalize_col(chunk["sexe"],           SEXE_MAP)
    if "gravite"        in chunk.columns: chunk["gravite"]        = normalize_col(chunk["gravite"],        GRAVITE_MAP)
    if "cat_usager"     in chunk.columns: chunk["cat_usager"]     = normalize_col(chunk["cat_usager"],     CAT_USAGER_MAP)
    if "place_vehicule" in chunk.columns: chunk["place_vehicule"] = normalize_col(chunk["place_vehicule"], PLACE_VEHICULE_MAP)
    return chunk

def load_dim_usager(engine):
    print("\n── DIM_USAGER")
    keep     = ["id_usager", "id_pays", "age", "sexe", "place_vehicule", "gravite", "cat_usager"]
    total_fr = 0
    fr_max   = 0
    for chunk in stream("dim_usager_fr.csv"):
        chunk    = normalize_usager(chunk)
        fr_max   = max(fr_max, int(chunk["id_usager"].max()))
        total_fr+= len(chunk)
        append_chunk(chunk, keep, "dim_usager", engine)
    print(f"    FR: {total_fr:,} rows  (max id_usager = {fr_max})")
    total_uk = 0
    for chunk in stream("dim_usager_uk.csv"):
        chunk = normalize_usager(chunk)
        chunk["id_usager"] = chunk["id_usager"] + fr_max
        total_uk += len(chunk)
        append_chunk(chunk, keep, "dim_usager", engine)
    print(f"    UK: {total_uk:,} rows  (offset +{fr_max})")
    print(f"  ✓ {total_fr + total_uk:,} total rows → dim_usager")
    return fr_max


# ─────────────────────────────────────────────────────────────
# 6. DIM_VEHICULE
# ─────────────────────────────────────────────────────────────

def normalize_vehicule(chunk):
    if "type_vehicule" in chunk.columns: chunk["type_vehicule"] = normalize_col(chunk["type_vehicule"], TYPE_VEHICULE_MAP)
    if "manoeuvre"     in chunk.columns: chunk["manoeuvre"]     = normalize_col(chunk["manoeuvre"],     MANOEUVRE_MAP)
    if "motorisation"  in chunk.columns: chunk["motorisation"]  = normalize_col(chunk["motorisation"],  MOTORISATION_MAP)
    return chunk

def load_dim_vehicule(engine):
    print("\n── DIM_VEHICULE")
    keep     = ["id_vehicule", "id_pays", "type_vehicule", "manoeuvre", "nb_occupants", "motorisation"]
    total_fr = 0
    fr_max   = 0
    for chunk in stream("dim_vehicule_fr.csv"):
        chunk    = normalize_vehicule(chunk)
        fr_max   = max(fr_max, int(chunk["id_vehicule"].max()))
        total_fr+= len(chunk)
        append_chunk(chunk, keep, "dim_vehicule", engine)
    print(f"    FR: {total_fr:,} rows  (max id_vehicule = {fr_max})")
    total_uk = 0
    for chunk in stream("dim_vehicule_uk.csv"):
        chunk = normalize_vehicule(chunk)
        chunk["id_vehicule"] = chunk["id_vehicule"] + fr_max
        total_uk += len(chunk)
        append_chunk(chunk, keep, "dim_vehicule", engine)
    print(f"    UK: {total_uk:,} rows  (offset +{fr_max})")
    print(f"  ✓ {total_fr + total_uk:,} total rows → dim_vehicule")
    return fr_max


# ─────────────────────────────────────────────────────────────
# 7. FAIT_ACCIDENT
# Granularity: one row per (id_pays, date) anchored on DIM_METEO.
# Casualty counts from DIM_USAGER aggregated per country,
# then distributed evenly across all dates for that country.
# ─────────────────────────────────────────────────────────────

def compute_fait_accident(engine):
    print("\n── FAIT_ACCIDENT")

    KILLED    = ("Killed",            "Tué")
    SERIOUS   = ("Seriously injured", "Blessé hospitalisé")
    SLIGHT    = ("Slightly injured",  "Blessé léger")

    # Step 1: aggregate usagers by (id_pays, gravite)
    print("    Aggregating usagers...")
    agg = pd.read_sql("""
        SELECT id_pays, gravite, COUNT(*) AS nb
        FROM dim_usager
        GROUP BY id_pays, gravite
    """, engine)

    pays_totals = {}
    for pays_id in agg["id_pays"].unique():
        sub = agg[agg["id_pays"] == pays_id]
        pays_totals[int(pays_id)] = {
            "nb_tues":           int(sub[sub["gravite"].isin(KILLED)  ]["nb"].sum()),
            "nb_blesses_graves": int(sub[sub["gravite"].isin(SERIOUS) ]["nb"].sum()),
            "nb_blesses_legers": int(sub[sub["gravite"].isin(SLIGHT)  ]["nb"].sum()),
            "nb_usagers":        int(sub["nb"].sum()),
        }

    # Step 2: use DIM_METEO as date anchor (one row per id_pays + date)
    print("    Loading meteo anchor...")
    meteo = pd.read_sql("SELECT id_meteo, id_pays, date FROM dim_meteo ORDER BY id_pays, date", engine)
    dates = pd.read_sql("SELECT id_date, date FROM dim_date", engine)

    meteo["date"] = pd.to_datetime(meteo["date"]).dt.date.astype(str)
    dates["date"] = pd.to_datetime(dates["date"]).dt.date.astype(str)
    meteo = meteo.merge(dates[["id_date", "date"]], on="date", how="left")

    # Step 3: build one fait row per (id_pays, date)
    fait_rows   = []
    accident_id = 0

    for pays_id, group in meteo.groupby("id_pays"):
        totals  = pays_totals.get(int(pays_id), {})
        n_dates = len(group)
        if n_dates == 0 or not totals:
            continue

        nb_tues   = totals["nb_tues"]
        nb_graves = totals["nb_blesses_graves"]
        nb_legers = totals["nb_blesses_legers"]
        nb_usagers= totals["nb_usagers"]

        d_tues    = round(nb_tues    / n_dates, 4)
        d_graves  = round(nb_graves  / n_dates, 4)
        d_legers  = round(nb_legers  / n_dates, 4)
        d_usagers = round(nb_usagers / n_dates, 4)

        for _, mrow in group.iterrows():
            accident_id += 1
            id_date = mrow.get("id_date")
            fait_rows.append({
                "id_accident":       accident_id,
                "id_pays":           int(pays_id),
                "id_date":           int(id_date) if pd.notna(id_date) else None,
                "id_meteo":          int(mrow["id_meteo"]),
                "nb_tues":           d_tues,
                "nb_blesses_graves": d_graves,
                "nb_blesses_legers": d_legers,
                "nb_victimes_total": round(d_tues + d_graves + d_legers, 4),
                "nb_usagers":        d_usagers,
                "indice_gravite":    round(d_tues*3 + d_graves*2 + d_legers, 4),
            })

        print(f"    pays {pays_id}: {n_dates:,} dates  "
              f"({nb_tues} killed, {nb_graves} serious, {nb_legers} slight total)")

    fait   = pd.DataFrame(fait_rows)
    before = len(fait)
    fait   = fait.dropna(subset=["id_date"])
    if before - len(fait):
        print(f"    ⚠ {before - len(fait):,} rows dropped (no matching date)")

    fait.to_sql("fait_accident", engine, if_exists="append", index=False)
    print(f"  ✓ {len(fait):,} rows → fait_accident")
    return len(fait)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("Connecting to database...")
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✓ Connected\n")

    load_dim_pays(engine)
    load_dim_date(engine)
    load_dim_meteo(engine)
    load_dim_localisation(engine)
    load_dim_vehicule(engine)
    load_dim_usager(engine)
    total_fait = compute_fait_accident(engine)

    print(f"\n{'═'*48}")
    print("✓ ETL complete")
    print(f"  FAIT_ACCIDENT : {total_fait:,} rows")

if __name__ == "__main__":
    main()