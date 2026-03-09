import requests
import os
import zipfile

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
RAW_DIR = "data/raw/meteo_uk"
os.makedirs(RAW_DIR, exist_ok=True)

ANNEES_CIBLES = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021]

KAGGLE_USERNAME = "11Maroua"
KAGGLE_KEY      = "KGAT_c2c337169be42000c5d86abba851f6b7"
DATASET         = "robjbutlermei/uk-daily-weather-1961-2024"

# ------------------------------------------------------------
# Telechargement via API Kaggle (sans librairie kaggle)
# ------------------------------------------------------------
print("=" * 60)
print("ETAPE 1 : Telechargement dataset Kaggle UK Daily Weather")
print("=" * 60)

url = f"https://www.kaggle.com/api/v1/datasets/download/{DATASET}"
chemin_zip = os.path.join(RAW_DIR, "uk_daily_weather.zip")

if os.path.exists(chemin_zip):
    print(f"[SKIP] Archive deja presente")
else:
    print(f"[DOWNLOAD] {DATASET}")
    r = requests.get(
        url,
        auth=(KAGGLE_USERNAME, KAGGLE_KEY),
        stream=True,
        timeout=300
    )
    if r.status_code == 200:
        taille = 0
        with open(chemin_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
                taille += len(chunk)
        print(f"[OK] {taille / 1024 / 1024:.1f} MB telecharges")
    else:
        print(f"[ERREUR] HTTP {r.status_code} : {r.text}")
        exit(1)

# ------------------------------------------------------------
# Etape 2 : Dezipper
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("ETAPE 2 : Extraction de l'archive")
print("=" * 60)

with zipfile.ZipFile(chemin_zip, "r") as z:
    fichiers = z.namelist()
    print(f"Fichiers dans l'archive : {fichiers}")
    z.extractall(RAW_DIR)
    print(f"[OK] Extraction terminee dans {RAW_DIR}")

os.remove(chemin_zip)

# ------------------------------------------------------------
# Etape 3 : Identifier le CSV et filtrer les annees
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("ETAPE 3 : Inspection et filtrage par annee")
print("=" * 60)

import pandas as pd

csv_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv") and not f.startswith("meteo_uk_")]
print(f"CSV trouves : {csv_files}")

for nom_csv in csv_files:
    chemin = os.path.join(RAW_DIR, nom_csv)
    print(f"\n[LECTURE] {nom_csv}")
    try:
        df = pd.read_csv(chemin, low_memory=False)
    except Exception as e:
        print(f"  ERREUR : {e}")
        continue

    print(f"  {len(df):,} lignes | colonnes : {list(df.columns[:6])}")

    # Identifier la colonne date
    col_date = next((c for c in df.columns if "date" in c.lower()), None)
    if col_date is None:
        print("  ATTENTION : colonne date introuvable")
        continue

    print(f"  Colonne date : '{col_date}' | exemple : {df[col_date].iloc[0]}")

    df = df.copy()
    df.loc[:, "_annee"] = pd.to_datetime(df[col_date], errors="coerce").dt.year

    annees_presentes = sorted(df["_annee"].dropna().astype(int).unique().tolist())
    print(f"  Annees presentes : {annees_presentes[0]} - {annees_presentes[-1]}")

    for annee in ANNEES_CIBLES:
        if annee not in annees_presentes:
            print(f"  [ABSENT] {annee}")
            continue
        nom_sortie    = f"meteo_uk_{annee}.csv"
        chemin_sortie = os.path.join(RAW_DIR, nom_sortie)
        df_annee      = df[df["_annee"] == annee].drop(columns=["_annee"])
        df_annee.to_csv(chemin_sortie, index=False)
        print(f"  [OK] {nom_sortie} ({len(df_annee):,} lignes)")

print("\n" + "=" * 60)
print("TERMINE")
print("=" * 60)