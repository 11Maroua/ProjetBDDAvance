"""
Build 3 dimension tables from UK accidents CSVs:
  - DIM_LOCALISATION  <- dft-road-casualty-statistics-collision-1979-latest-published-year.csv
  - DIM_VEHICULE      <- dft-road-casualty-statistics-vehicle-1979-latest-published-year.csv
  - DIM_USAGER        <- dft-road-casualty-statistics-casualty-1979-latest-published-year.csv
"""

import pandas as pd

CHUNK_SIZE = 100_000
ID_PAYS    = 826  # ISO 3166-1 numeric for United Kingdom

BASE = "data/raw/accidents_uk"
COLLISION_CSV = f"{BASE}/dft-road-casualty-statistics-collision-1979-latest-published-year.csv"
VEHICLE_CSV   = f"{BASE}/dft-road-casualty-statistics-vehicle-1979-latest-published-year.csv"
CASUALTY_CSV  = f"{BASE}/dft-road-casualty-statistics-casualty-1979-latest-published-year.csv"

# ── Lookup mappings ────────────────────────────────────────────────────────────

ROAD_CLASS_MAP = {
    1: "Motorway", 2: "A(M) Road", 3: "A Road",
    4: "B Road",   5: "C Road",    6: "Unclassified", 0: "Unknown",
}
VEHICLE_TYPE_MAP = {
    1: "Pedal cycle", 2: "Motorcycle 50cc and under", 3: "Motorcycle 125cc and under",
    4: "Motorcycle over 125cc and up to 500cc", 5: "Motorcycle over 500cc",
    8: "Taxi/Private hire car", 9: "Car", 10: "Minibus (8-16 passenger seats)",
    11: "Bus or coach (17+ passenger seats)", 16: "Ridden horse",
    17: "Agricultural vehicle", 18: "Tram", 19: "Van/Goods vehicle 3.5t and under",
    20: "Goods vehicle over 3.5t and up to 7.5t", 21: "Goods vehicle over 7.5t",
    22: "Mobility scooter", 23: "Electric motorcycle", 90: "Other vehicle",
    97: "Motorcycle - unknown cc", 98: "Goods vehicle - unknown weight", -1: "Unknown",
}
MANOEUVRE_MAP = {
    1: "Reversing", 2: "Parked", 3: "Waiting to go ahead",
    4: "Slowing or stopping", 5: "Moving off", 6: "U-turn",
    7: "Turning left", 8: "Waiting to turn left", 9: "Turning right",
    10: "Waiting to turn right", 11: "Changing lane to left",
    12: "Changing lane to right", 13: "Overtaking moving vehicle on offside",
    14: "Overtaking static vehicle on offside", 15: "Overtaking on nearside",
    16: "Going ahead - masked", 17: "Going ahead other",
    18: "Going ahead - masked (historic)", 19: "Going ahead", -1: "Unknown",
}
PROPULSION_MAP = {
    1: "Petrol", 2: "Heavy oil", 3: "Electric", 4: "Steam",
    5: "Gas", 6: "Petrol/Gas hybrid", 7: "Gas/Bi-fuel",
    8: "Hybrid electric", 9: "Gas diesel", 10: "New fuel technology",
    11: "Fuel cells", 12: "Electric diesel", -1: "Unknown", 0: "Unknown",
}
CASUALTY_CLASS_MAP = {1: "Driver or rider", 2: "Passenger", 3: "Pedestrian", -1: "Unknown"}
SEX_MAP            = {1: "Male", 2: "Female", -1: "Unknown", 9: "Unknown"}
SEVERITY_MAP       = {1: "Fatal", 2: "Serious", 3: "Slight", -1: "Unknown"}
CAR_PASSENGER_MAP  = {0: "Not car passenger", 1: "Front seat", 2: "Rear seat", -1: "Unknown"}
BUS_PASSENGER_MAP  = {0: "Not bus passenger", 1: "Boarding/alighting",
                      2: "Standing passenger", 3: "Seated passenger", -1: "Unknown"}


# ── Generic chunked processor ──────────────────────────────────────────────────

def process_chunks(csv_path, usecols, transform_fn, dedup_cols, label):
    print(f"\n[{label}] {csv_path}")
    seen, rows, total = set(), [], 0

    for i, chunk in enumerate(pd.read_csv(
            csv_path, encoding="utf-8-sig",
            usecols=usecols, chunksize=CHUNK_SIZE, low_memory=False)):

        total += len(chunk)
        chunk = transform_fn(chunk)

        for row in chunk[dedup_cols].drop_duplicates().itertuples(index=False):
            key = tuple(row)
            if key not in seen:
                seen.add(key)
                rows.append(row)

        print(f"  chunk {i+1}: {total:,} read | {len(rows):,} unique")

    dim = pd.DataFrame(rows, columns=dedup_cols).reset_index(drop=True)
    print(f"  => {len(dim):,} rows")
    return dim


# ── 1. DIM_LOCALISATION ────────────────────────────────────────────────────────

def transform_localisation(c):
    c["type_route"]     = c["first_road_class"].map(ROAD_CLASS_MAP).fillna("Unknown")
    c["vitesse_limite"] = pd.to_numeric(c["speed_limit"], errors="coerce").replace(-1, pd.NA)
    c["commune"]        = c["local_authority_ons_district"].astype(str)
    c["departement"]    = "N/A"
    c["region"]         = "N/A"
    c["id_pays"]        = ID_PAYS
    c["latitude"]       = c["latitude"].round(4)
    c["longitude"]      = c["longitude"].round(4)
    return c

dim_loc = process_chunks(
    COLLISION_CSV,
    ["longitude", "latitude", "first_road_class", "speed_limit", "local_authority_ons_district"],
    transform_localisation,
    ["id_pays", "commune", "departement", "region", "type_route", "vitesse_limite", "latitude", "longitude"],
    "DIM_LOCALISATION"
)
dim_loc.insert(0, "id_lieu", range(1, len(dim_loc) + 1))
dim_loc[["id_lieu","id_pays","commune","departement","region",
         "type_route","vitesse_limite","latitude","longitude"]].to_csv("dim_localisation.csv", index=False)
print("  Saved -> dim_localisation.csv")


# ── 2. DIM_VEHICULE ────────────────────────────────────────────────────────────

def transform_vehicule(c):
    c["type_vehicule"] = c["vehicle_type"].map(VEHICLE_TYPE_MAP).fillna("Unknown")
    c["manoeuvre"]     = c["vehicle_manoeuvre"].map(MANOEUVRE_MAP).fillna("Unknown")
    c["motorisation"]  = c["propulsion_code"].map(PROPULSION_MAP).fillna("Unknown")
    c["nb_occupants"]  = pd.to_numeric(c["towing_and_articulation"], errors="coerce").replace(-1, pd.NA)
    c["id_pays"]       = ID_PAYS
    return c

dim_veh = process_chunks(
    VEHICLE_CSV,
    ["vehicle_type", "vehicle_manoeuvre", "propulsion_code", "towing_and_articulation"],
    transform_vehicule,
    ["id_pays", "type_vehicule", "manoeuvre", "nb_occupants", "motorisation"],
    "DIM_VEHICULE"
)
dim_veh.insert(0, "id_vehicule", range(1, len(dim_veh) + 1))
dim_veh[["id_vehicule","id_pays","type_vehicule","manoeuvre",
         "nb_occupants","motorisation"]].to_csv("dim_vehicule.csv", index=False)
print("  Saved -> dim_vehicule.csv")


# ── 3. DIM_USAGER ──────────────────────────────────────────────────────────────

def transform_usager(c):
    c["age"]        = pd.to_numeric(c["age_of_casualty"], errors="coerce").replace(-1, pd.NA)
    c["sexe"]       = c["sex_of_casualty"].map(SEX_MAP).fillna("Unknown")
    c["gravite"]    = c["casualty_severity"].map(SEVERITY_MAP).fillna("Unknown")
    c["cat_usager"] = c["casualty_class"].map(CASUALTY_CLASS_MAP).fillna("Unknown")

    def place(row):
        if row["car_passenger"] not in (0, -1):
            return CAR_PASSENGER_MAP.get(row["car_passenger"], "Unknown")
        if row["bus_or_coach_passenger"] not in (0, -1):
            return BUS_PASSENGER_MAP.get(row["bus_or_coach_passenger"], "Unknown")
        return "Not passenger"

    c["place_vehicule"] = c.apply(place, axis=1)
    c["id_pays"]        = ID_PAYS
    return c

dim_usg = process_chunks(
    CASUALTY_CSV,
    ["age_of_casualty", "sex_of_casualty", "casualty_class",
     "casualty_severity", "car_passenger", "bus_or_coach_passenger"],
    transform_usager,
    ["id_pays", "age", "sexe", "place_vehicule", "gravite", "cat_usager"],
    "DIM_USAGER"
)
dim_usg.insert(0, "id_usager", range(1, len(dim_usg) + 1))
dim_usg[["id_usager","id_pays","age","sexe",
         "place_vehicule","gravite","cat_usager"]].to_csv("dim_usager.csv", index=False)
print("  Saved -> dim_usager.csv")

print("\nAll 3 dimensions built successfully.")