"""
build_dims_and_fait.py
======================
Builds all dimension CSVs and fait_accident.csv in one pass
from raw accident files, ensuring IDs are consistent throughout.

Output files (written to OUTPUT_DIR):
  fait_accident.csv
  dim_localisation.csv
  dim_usager.csv
  dim_vehicule.csv
  dim_pays.csv        (static — 2 rows)
  dim_date.csv        (copied from DIM_TEMPS.csv)
  dim_meteo.csv       (copied from dim_meteo source)

FR files (one per year):
  RAW_DIR/accidents_fr/caracteristiques_YEAR.csv
  RAW_DIR/accidents_fr/lieux_YEAR.csv
  RAW_DIR/accidents_fr/usagers_YEAR.csv
  RAW_DIR/accidents_fr/vehicules_YEAR.csv

UK files (one per year, produced by download_uk.py):
  RAW_DIR/accidents_uk/dft-road-casualty-statistics-collision-YEAR.csv
  RAW_DIR/accidents_uk/dft-road-casualty-statistics-casualty-YEAR.csv
  RAW_DIR/accidents_uk/dft-road-casualty-statistics-vehicle-YEAR.csv

Usage:
  pip install pandas pyproj
  python build_dims_and_fait.py
"""

import os
import csv as csv_mod
import pandas as pd
import shutil
from pyproj import Transformer

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
RAW_DIR         = "./data/raw"
DIM_SOURCE_DIR  = "./data/processed"          # existing dim_meteo.csv and DIM_TEMPS.csv
OUTPUT_DIR      = "./data/dims"     # all output CSVs go here
YEARS           = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021]

# ─────────────────────────────────────────────────────────────
# DECODE TABLES
# ─────────────────────────────────────────────────────────────

FR_GRAV  = {1: "Uninjured", 2: "Killed", 3: "Seriously injured", 4: "Slightly injured"}
FR_SEXE  = {1: "Male", 2: "Female", -1: "Unknown"}
FR_CATU  = {1: "Driver", 2: "Passenger", 3: "Pedestrian", 4: "Cyclist"}
FR_CATV  = {
    1: "Bicycle", 2: "Moped", 3: "Motorcycle", 4: "Motorcycle",
    5: "Motorcycle", 6: "Motorcycle", 7: "Car", 10: "Car",
    13: "Heavy goods vehicle", 14: "Heavy goods vehicle",
    15: "Heavy goods vehicle", 16: "Tractor", 17: "Agricultural vehicle",
    20: "Scooter", 21: "Bicycle", 30: "Electric scooter",
    31: "Electric bicycle", 40: "Tram", 41: "Bus", 42: "Train", 99: "Other",
}
FR_MANV  = {
    0: "Unknown", 1: "Going ahead", 2: "Going ahead", 3: "Going ahead",
    4: "Going ahead", 5: "Going ahead", 6: "Going ahead", 7: "Going ahead",
    8: "Going ahead", 9: "Turning left", 10: "Turning right", 11: "U-turn",
    12: "Changing lane", 13: "Overtaking", 14: "Overtaking", 15: "Overtaking",
    17: "Crossing median", 18: "Reversing", 19: "Parked", 22: "Parked",
    24: "Avoiding obstacle", 25: "Door opening", 26: "Stopped",
}
FR_MOTOR = {
    0: "Unknown", 1: "Petrol", 2: "Hybrid", 3: "Electric",
    4: "Hydrogen", 5: "Human powered", 6: "Other", -1: "Unknown",
}
FR_CATR  = {
    1: "Motorway", 2: "National road", 3: "Departmental road",
    4: "Municipal road", 5: "Off-network", 6: "Parking",
    7: "Urban road", 9: "Other",
}

UK_SEV   = {1: "Killed", 2: "Seriously injured", 3: "Slightly injured"}
UK_SEXE  = {1: "Male", 2: "Female", -1: "Unknown", 3: "Unknown"}
UK_CLASS = {1: "Driver", 2: "Passenger", 3: "Pedestrian"}
UK_VEH   = {
    1: "Bicycle", 2: "Motorcycle", 3: "Motorcycle", 4: "Taxi",
    5: "Bus", 8: "Motorcycle", 9: "Car", 10: "Minibus", 11: "Bus",
    16: "Ridden horse", 17: "Agricultural vehicle", 18: "Tram",
    19: "Van", 20: "Heavy goods vehicle", 21: "Heavy goods vehicle",
    90: "Other", 99: "Unknown", 109: "Car",
}
UK_MANV  = {
    1: "Reversing", 2: "Parked", 3: "Waiting to go ahead",
    4: "Slowing or stopping", 5: "Moving off", 6: "U-turn",
    7: "Turning left", 8: "Waiting to turn left", 9: "Turning right",
    10: "Waiting to turn right", 11: "Changing lane to left",
    12: "Changing lane to right", 13: "Overtaking", 14: "Overtaking",
    16: "Going ahead", 17: "Going ahead", 99: "Unknown",
}
UK_PROP  = {
    0: "Unknown", 1: "Petrol", 2: "Diesel", 3: "Electric",
    4: "Steam", 5: "Gas", 6: "Petrol/Gas", 7: "Gas/Bi-fuel",
    8: "Hybrid", 9: "Gas diesel", 10: "New fuel technology",
    11: "Fuel cells", 12: "Electric diesel", -1: "Unknown",
}
UK_ROAD  = {
    1: "Motorway", 2: "A road", 3: "A road",
    4: "B road", 5: "C road", 6: "Unclassified",
}

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

lambert = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)

def fp(*parts):
    return os.path.join(*parts)

def detect_params(filepath):
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
                    df = pd.read_csv(filepath, nrows=5, encoding=encoding,
                                     sep=sep, low_memory=False)
                    if len(df.columns) > 1:
                        return encoding, sep
                except Exception:
                    continue
    return "latin-1", ","

def read(filepath):
    encoding, sep = detect_params(filepath)
    df = pd.read_csv(filepath, low_memory=False, encoding=encoding,
                     sep=sep, on_bad_lines="skip")
    for col in df.select_dtypes(include="object").columns:
        df[col] = (df[col].astype(str)
                          .str.replace("\xa0", "", regex=False)
                          .str.strip()
                          .replace({"nan": None, "None": None, "N/A": None}))
    return df

def decode(val, codebook):
    try:
        return codebook.get(int(float(val)), "Unknown")
    except (ValueError, TypeError):
        return "Unknown"

def safe_int(val):
    try:
        v = int(float(val))
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None

def convert_lambert(lat, lon):
    try:
        raw_lat = float(str(lat).replace(",", "."))
        raw_lon = float(str(lon).replace(",", "."))
        if abs(raw_lat) > 90:
            lon_wgs, lat_wgs = lambert.transform(raw_lon, raw_lat)
            return round(lat_wgs, 6), round(lon_wgs, 6)
        return round(raw_lat, 6), round(raw_lon, 6)
    except (ValueError, TypeError):
        return None, None

# ─────────────────────────────────────────────────────────────
# SURROGATE KEY COUNTERS
# ─────────────────────────────────────────────────────────────

_counters = {"id_accident": 0, "id_lieu": 0, "id_usager": 0, "id_vehicule": 0}

def nid(key):
    _counters[key] += 1
    return _counters[key]

# ─────────────────────────────────────────────────────────────
# OUTPUT FILE WRITERS
# ─────────────────────────────────────────────────────────────

_handles      = {}
_first_writes = {}

def open_outputs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    files = {
        "fait":  fp(OUTPUT_DIR, "fait_accident.csv"),
        "loc":   fp(OUTPUT_DIR, "dim_localisation.csv"),
        "usager":fp(OUTPUT_DIR, "dim_usager.csv"),
        "veh":   fp(OUTPUT_DIR, "dim_vehicule.csv"),
    }
    for key, path in files.items():
        _handles[key]      = open(path, "w", newline="", encoding="utf-8")
        _first_writes[key] = True

def write(key, df):
    if df is None or len(df) == 0:
        return
    df.to_csv(_handles[key], header=_first_writes[key], index=False)
    _first_writes[key] = False

def close_outputs():
    for fh in _handles.values():
        fh.close()

# ─────────────────────────────────────────────────────────────
# LOAD DIMENSION LOOKUPS  (date + meteo)
# ─────────────────────────────────────────────────────────────

def load_lookups():
    print("Loading lookups (meteo only — date stored as string)...")
    meteo = read(fp(DIM_SOURCE_DIR, "dim_meteo.csv"))
    meteo_map = {
        (int(row["id_pays"]), str(row["date"])[:10]): int(row["id_meteo"])
        for _, row in meteo.iterrows()
    }
    print(f"  ✓ {len(meteo_map):,} meteo entries\n")
    return meteo_map

# ─────────────────────────────────────────────────────────────
# PROCESS ONE FR YEAR
# ─────────────────────────────────────────────────────────────

def process_fr_year(year, meteo_map):
    print(f"  [FR {year}]", end=" ", flush=True)

    caract  = read(fp(RAW_DIR, "accidents_fr", f"caracteristiques_{year}.csv"))
    lieux   = read(fp(RAW_DIR, "accidents_fr", f"lieux_{year}.csv"))
    usagers = read(fp(RAW_DIR, "accidents_fr", f"usagers_{year}.csv"))
    vehs    = read(fp(RAW_DIR, "accidents_fr", f"vehicules_{year}.csv"))

    # FR files store year as 2-digit (5 → 2005) or 4-digit (2021)
    # Normalize to full 4-digit year using the known year parameter
    caract["_date"] = (
        str(year) + "-" +
        caract["mois"].astype(str).str.zfill(2) + "-" +
        caract["jour"].astype(str).str.zfill(2)
    )
    merged = caract.merge(lieux, on="Num_Acc", how="left")

    # ── DIM_LOCALISATION
    loc_rows = []
    loc_map  = {}
    for _, row in merged.iterrows():
        new_id = nid("id_lieu")
        loc_map[row["Num_Acc"]] = new_id
        lat, lon = convert_lambert(row.get("lat"), row.get("long"))
        loc_rows.append({
            "id_lieu":        new_id,
            "id_pays":        1,
            "commune":        str(safe_int(row.get("com"))) if safe_int(row.get("com")) else None,
            "departement":    str(row.get("dep", ""))[:20]  if pd.notna(row.get("dep")) else None,
            "district":       None,
            "type_route":     decode(row.get("catr"), FR_CATR),
            "vitesse_limite": safe_int(row.get("vma")),
            "latitude":       lat,
            "longitude":      lon,
        })

    # ── DIM_USAGER
    usager_rows = []
    usager_map  = {}
    for _, row in usagers.iterrows():
        new_id = nid("id_usager")
        acc    = row["Num_Acc"]
        birth  = row.get("an_nais")
        age    = (year - int(float(birth))) \
                 if pd.notna(birth) and str(birth).replace(".","").lstrip("-").isdigit() \
                 else None
        grav   = decode(row.get("grav"), FR_GRAV)
        usager_rows.append({
            "id_usager":      new_id,
            "id_pays":        1,
            "age":            age,
            "sexe":           decode(row.get("sexe"), FR_SEXE),
            "place_vehicule": str(safe_int(row.get("place"))) if safe_int(row.get("place")) else None,
            "gravite":        grav,
            "cat_usager":     decode(row.get("catu"), FR_CATU),
        })
        if acc not in usager_map:
            usager_map[acc] = {"id_usager": new_id,
                               "id_veh_src": str(row.get("id_vehicule", "")).strip()}

    # ── DIM_VEHICULE
    veh_rows = []
    veh_map  = {}
    for _, row in vehs.iterrows():
        new_id  = nid("id_vehicule")
        key     = (row["Num_Acc"], str(row.get("id_vehicule", "")).strip())
        if key not in veh_map:
            veh_map[key] = new_id
        veh_rows.append({
            "id_vehicule":   new_id,
            "id_pays":       1,
            "type_vehicule": decode(row.get("catv"),  FR_CATV),
            "manoeuvre":     decode(row.get("manv"),  FR_MANV),
            "nb_occupants":  safe_int(row.get("occutc")) or 0,
            "motorisation":  decode(row.get("motor"), FR_MOTOR),
        })

    # ── FAIT_ACCIDENT
    usagers["_grav"] = usagers["grav"].apply(lambda x: decode(x, FR_GRAV))
    grav_agg  = usagers.groupby("Num_Acc")["_grav"].apply(list)
    veh_count = vehs.groupby("Num_Acc").size()
    date_idx  = dict(zip(caract["Num_Acc"], caract["_date"]))

    fait_rows = []
    for _, row in caract.iterrows():
        acc      = row["Num_Acc"]
        date_str = date_idx.get(acc, "")
        gravs    = grav_agg.get(acc, [])
        nb_tues  = sum(1 for g in gravs if g == "Killed")
        nb_grav  = sum(1 for g in gravs if g == "Seriously injured")
        nb_leg   = sum(1 for g in gravs if g == "Slightly injured")
        u_info   = usager_map.get(acc, {})
        veh_key  = (acc, u_info.get("id_veh_src", ""))
        fait_rows.append({
            "id_accident":       nid("id_accident"),
            "id_pays":           1,
            "date":              date_str,
            "id_lieu":           loc_map.get(acc),
            "id_usager":         u_info.get("id_usager"),
            "id_vehicule":       veh_map.get(veh_key),
            "id_meteo":          meteo_map.get((1, date_str)),
            "nb_tues":           nb_tues,
            "nb_blesses_graves": nb_grav,
            "nb_blesses_legers": nb_leg,
            "nb_victimes_total": nb_tues + nb_grav + nb_leg,
            "nb_vehicules":      int(veh_count.get(acc, 1)),
            "indice_gravite":    nb_tues * 3 + nb_grav * 2 + nb_leg,
        })

    print(f"{len(fait_rows):,} accidents  "
          f"{len(usager_rows):,} usagers  "
          f"{len(veh_rows):,} vehicules")

    write("loc",    pd.DataFrame(loc_rows))
    write("usager", pd.DataFrame(usager_rows))
    write("veh",    pd.DataFrame(veh_rows))
    return pd.DataFrame(fait_rows)

# ─────────────────────────────────────────────────────────────
# PROCESS ONE UK YEAR
# ─────────────────────────────────────────────────────────────

def process_uk_year(year, meteo_map):
    print(f"  [UK {year}]", end=" ", flush=True)

    col_file = fp(RAW_DIR, "accidents_uk",
                  f"dft-road-casualty-statistics-collision-{year}.csv")
    cas_file = fp(RAW_DIR, "accidents_uk",
                  f"dft-road-casualty-statistics-casualty-{year}.csv")
    veh_file = fp(RAW_DIR, "accidents_uk",
                  f"dft-road-casualty-statistics-vehicle-{year}.csv")

    for f_ in [col_file, cas_file, veh_file]:
        if not os.path.exists(f_):
            print(f"SKIP (missing {os.path.basename(f_)})")
            return pd.DataFrame()

    collision = read(col_file)
    casualty  = read(cas_file)
    vehicle   = read(veh_file)

    # Resolve index column name (varies by year)
    def idx_col(df):
        for c in ["collision_index", "accident_index"]:
            if c in df.columns:
                return c
        return None

    col_idx = idx_col(collision)
    cas_idx = idx_col(casualty)
    veh_idx = idx_col(vehicle)
    if not col_idx:
        print("SKIP (no index column)")
        return pd.DataFrame()

    # Parse date
    date_col = next((c for c in collision.columns if "date" in c.lower()), None)
    if date_col:
        collision["_date"] = pd.to_datetime(
            collision[date_col].astype(str), dayfirst=True, errors="coerce"
        ).dt.strftime("%Y-%m-%d")
    else:
        collision["_date"] = None

    # ── DIM_LOCALISATION
    loc_rows = []
    loc_map  = {}
    for _, row in collision.iterrows():
        new_id = nid("id_lieu")
        loc_map[row[col_idx]] = new_id
        loc_rows.append({
            "id_lieu":        new_id,
            "id_pays":        2,
            "commune":        None,
            "departement":    None,
            "district":       str(row.get("local_authority_ons_district", ""))[:50] or None,
            "type_route":     decode(row.get("first_road_class"), UK_ROAD),
            "vitesse_limite": safe_int(row.get("speed_limit")),
            "latitude":       round(float(row["latitude"]),  6) if pd.notna(row.get("latitude"))  else None,
            "longitude":      round(float(row["longitude"]), 6) if pd.notna(row.get("longitude")) else None,
        })

    # ── DIM_USAGER
    usager_rows = []
    usager_map  = {}
    sev_col = next((c for c in casualty.columns
                    if "severity" in c.lower() and "casualty" in c.lower()), None)
    sex_col = next((c for c in casualty.columns
                    if "sex" in c.lower() and "casualty" in c.lower()), None)
    age_col = next((c for c in casualty.columns
                    if "age" in c.lower() and "casualty" in c.lower()
                    and "band" not in c.lower()), None)
    cls_col = next((c for c in casualty.columns
                    if "class" in c.lower() and "casualty" in c.lower()), None)

    for _, row in casualty.iterrows():
        new_id = nid("id_usager")
        cidx   = row[cas_idx]
        grav   = decode(row.get(sev_col), UK_SEV) if sev_col else "Unknown"
        usager_rows.append({
            "id_usager":      new_id,
            "id_pays":        2,
            "age":            safe_int(row.get(age_col)) if age_col else None,
            "sexe":           decode(row.get(sex_col), UK_SEXE) if sex_col else "Unknown",
            "place_vehicule": decode(row.get(cls_col), UK_CLASS) if cls_col else None,
            "gravite":        grav,
            "cat_usager":     decode(row.get(cls_col), UK_CLASS) if cls_col else "Unknown",
        })
        if cidx not in usager_map:
            usager_map[cidx] = new_id

    # ── DIM_VEHICULE
    veh_rows = []
    veh_map  = {}
    veh_count= {}
    veh_type_col = next((c for c in vehicle.columns if "vehicle_type" in c.lower()), None)
    manv_col     = next((c for c in vehicle.columns if "manoeuvre" in c.lower()
                         and "historic" not in c.lower()), None)
    prop_col     = next((c for c in vehicle.columns if "propulsion" in c.lower()), None)

    for _, row in vehicle.iterrows():
        new_id = nid("id_vehicule")
        cidx   = row[veh_idx]
        if cidx not in veh_map:
            veh_map[cidx] = new_id
        veh_count[cidx] = veh_count.get(cidx, 0) + 1
        veh_rows.append({
            "id_vehicule":   new_id,
            "id_pays":       2,
            "type_vehicule": decode(row.get(veh_type_col), UK_VEH)  if veh_type_col else "Unknown",
            "manoeuvre":     decode(row.get(manv_col),     UK_MANV) if manv_col     else "Unknown",
            "nb_occupants":  0,
            "motorisation":  decode(row.get(prop_col),     UK_PROP) if prop_col     else "Unknown",
        })

    # ── FAIT_ACCIDENT
    if sev_col:
        casualty["_grav"] = casualty[sev_col].apply(lambda x: decode(x, UK_SEV))
        grav_agg = casualty.groupby(cas_idx)["_grav"].apply(list)
    else:
        grav_agg = {}

    date_idx = dict(zip(collision[col_idx], collision["_date"]))

    fait_rows = []
    for _, row in collision.iterrows():
        cidx     = row[col_idx]
        date_str = str(date_idx.get(cidx, ""))
        gravs    = grav_agg.get(cidx, []) if hasattr(grav_agg, "get") else []
        nb_tues  = sum(1 for g in gravs if g == "Killed")
        nb_grav  = sum(1 for g in gravs if g == "Seriously injured")
        nb_leg   = sum(1 for g in gravs if g == "Slightly injured")
        fait_rows.append({
            "id_accident":       nid("id_accident"),
            "id_pays":           2,
            "date":              date_str,
            "id_lieu":           loc_map.get(cidx),
            "id_usager":         usager_map.get(cidx),
            "id_vehicule":       veh_map.get(cidx),
            "id_meteo":          meteo_map.get((2, date_str)),
            "nb_tues":           nb_tues,
            "nb_blesses_graves": nb_grav,
            "nb_blesses_legers": nb_leg,
            "nb_victimes_total": nb_tues + nb_grav + nb_leg,
            "nb_vehicules":      veh_count.get(cidx, 1),
            "indice_gravite":    nb_tues * 3 + nb_grav * 2 + nb_leg,
        })

    print(f"{len(fait_rows):,} accidents  "
          f"{len(usager_rows):,} usagers  "
          f"{len(veh_rows):,} vehicules")

    write("loc",    pd.DataFrame(loc_rows))
    write("usager", pd.DataFrame(usager_rows))
    write("veh",    pd.DataFrame(veh_rows))
    return pd.DataFrame(fait_rows)

# ─────────────────────────────────────────────────────────────
# FINALIZE FAIT BEFORE WRITING
# ─────────────────────────────────────────────────────────────

def finalize_fait(fait):
    if fait is None or len(fait) == 0:
        return fait
    fait = fait.dropna(subset=["date", "id_lieu", "id_usager", "id_vehicule"])
    for col in ["id_accident", "id_pays", "id_lieu", "id_usager", "id_vehicule"]:
        fait[col] = fait[col].astype(int)
    return fait

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    meteo_map = load_lookups()

    open_outputs()
    total_fait = 0

    try:
        # ── FR
        print("Processing FR years...")
        for year in YEARS:
            fait = process_fr_year(year, meteo_map)
            fait = finalize_fait(fait)
            write("fait", fait)
            total_fait += len(fait)

        # ── UK
        print("\nProcessing UK years...")
        for year in YEARS:
            fait = process_uk_year(year, meteo_map)
            fait = finalize_fait(fait)
            write("fait", fait)
            total_fait += len(fait)

    finally:
        close_outputs()

    # ── Transform and write dim_date from DIM_TEMPS
    temps = read(fp(DIM_SOURCE_DIR, "DIM_TEMPS.csv"))
    if "id_temps" in temps.columns:
        temps = temps.rename(columns={"id_temps": "id_date"})
    if "est_ferie_fr" in temps.columns:
        temps = temps.rename(columns={"est_ferie_fr": "est_jour_ferie"})
    temps = temps.drop(columns=["est_ferie_uk", "trimestre", "nom_jour",
                                 "nom_mois", "jour_semaine"], errors="ignore")
    if "heure" not in temps.columns:
        temps["heure"] = None
    for col in ["est_weekend", "est_jour_ferie"]:
        if col in temps.columns:
            temps[col] = temps[col].astype(float).astype("boolean")
    keep = ["id_date", "date", "annee", "mois", "jour", "heure",
            "saison", "est_weekend", "est_jour_ferie"]
    temps[[c for c in keep if c in temps.columns]].to_csv(
        fp(OUTPUT_DIR, "dim_date.csv"), index=False)
    print(f"\n  ✓ Written dim_date.csv ({len(temps):,} rows)")

    # ── Transform and write dim_meteo
    meteo = read(fp(DIM_SOURCE_DIR, "dim_meteo.csv"))
    meteo = meteo.rename(columns={"T_min": "temp_min", "T_max": "temp_max"})
    keep_m = ["id_meteo", "id_pays", "date", "temp_min", "temp_max",
              "precipitations", "vent", "conditions"]
    meteo[[c for c in keep_m if c in meteo.columns]].to_csv(
        fp(OUTPUT_DIR, "dim_meteo.csv"), index=False)
    print(f"  ✓ Written dim_meteo.csv ({len(meteo):,} rows)")

    # ── Write dim_pays (static)
    pd.DataFrame([
        {"id_pays": 1, "code_pays": "FR", "nom_pays": "France"},
        {"id_pays": 2, "code_pays": "UK", "nom_pays": "United Kingdom"},
    ]).to_csv(fp(OUTPUT_DIR, "dim_pays.csv"), index=False)
    print("  ✓ Written dim_pays.csv")

    print(f"\n{'═'*52}")
    print(f"✓ All files written to: {OUTPUT_DIR}")
    print(f"  fait_accident.csv   : {total_fait:,} rows")
    print(f"  id_accident max     : {_counters['id_accident']:,}")
    print(f"  id_lieu max         : {_counters['id_lieu']:,}")
    print(f"  id_usager max       : {_counters['id_usager']:,}")
    print(f"  id_vehicule max     : {_counters['id_vehicule']:,}")
    print(f"\nNext step: run load_db.py to load all CSVs into PostgreSQL")

if __name__ == "__main__":
    main()