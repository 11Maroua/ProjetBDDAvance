import requests
import os
import shutil
import pandas as pd

RAW_DIR = "data/raw/accidents_uk"
os.makedirs(RAW_DIR, exist_ok=True)

ANNEES_CIBLES = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021]
BASE_URL = "https://data.dft.gov.uk/road-accidents-safety-data"

FICHIERS_COMPLETS = {
    "collision": "dft-road-casualty-statistics-collision-1979-latest-published-year.csv",
    "casualty":  "dft-road-casualty-statistics-casualty-1979-latest-published-year.csv",
    "vehicle":   "dft-road-casualty-statistics-vehicle-1979-latest-published-year.csv",
}

# Colonne annee selon le type de fichier
COLONNE_ANNEE = {
    "collision": "accident_year",
    "casualty":  "accident_year",
    "vehicle":   "accident_year",
}

# ------------------------------------------------------------
# Fonctions
# ------------------------------------------------------------
def telecharger_fichier(url, chemin_dest):
    print(f"[DOWNLOAD] {os.path.basename(chemin_dest)}")
    print(f"           URL : {url}")
    try:
        r = requests.get(url, stream=True, timeout=300)
        r.raise_for_status()
        taille = 0
        with open(chemin_dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
                taille += len(chunk)
        print(f"           OK  : {taille / 1024 / 1024:.1f} MB telecharges")
        return True
    except requests.HTTPError as e:
        print(f"           ERREUR HTTP : {e}")
    except Exception as e:
        print(f"           ERREUR : {e}")
    return False


def extraire_annees(chemin_source, type_fichier, annees):
    print(f"\n[EXTRACTION] {type_fichier} - lecture du fichier complet...")
    col_annee = COLONNE_ANNEE[type_fichier]
    try:
        df = pd.read_csv(chemin_source, low_memory=False)
    except Exception as e:
        print(f"  ERREUR lecture : {e}")
        return

    if col_annee not in df.columns:
        # Tentative avec variante de nom
        cols_disponibles = [c for c in df.columns if "year" in c.lower()]
        print(f"  ATTENTION : colonne '{col_annee}' introuvable.")
        print(f"  Colonnes avec 'year' disponibles : {cols_disponibles}")
        if cols_disponibles:
            col_annee = cols_disponibles[0]
            print(f"  Utilisation de '{col_annee}' a la place.")
        else:
            print("  Impossible d'identifier la colonne annee. Abandon.")
            return

    print(f"  {len(df):,} lignes chargees. Extraction par annee...")
    for annee in annees:
        nom_sortie = f"dft-road-casualty-statistics-{type_fichier}-{annee}.csv"
        chemin_sortie = os.path.join(RAW_DIR, nom_sortie)
        if os.path.exists(chemin_sortie):
            print(f"  [SKIP] {nom_sortie} (deja present)")
            continue
        df_annee = df[df[col_annee] == annee]
        df_annee.to_csv(chemin_sortie, index=False)
        print(f"  [OK]   {nom_sortie} ({len(df_annee):,} lignes)")


# ------------------------------------------------------------
# Etape 1 : Telecharger les fichiers complets si absents
# ------------------------------------------------------------
print("ETAPE 1 : Telechargement des fichiers complets (1979-2024)")

for type_fichier, nom_fichier in FICHIERS_COMPLETS.items():
    chemin = os.path.join(RAW_DIR, nom_fichier)
    if os.path.exists(chemin):
        print(f"[SKIP] {nom_fichier} (deja present)")
        continue
    url = f"{BASE_URL}/{nom_fichier}"
    telecharger_fichier(url, chemin)

# ------------------------------------------------------------
# Etape 2 : Extraire les annees cibles depuis chaque fichier
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("ETAPE 2 : Extraction des annees", ANNEES_CIBLES)
print("=" * 60)

for type_fichier, nom_fichier in FICHIERS_COMPLETS.items():
    chemin = os.path.join(RAW_DIR, nom_fichier)
    if not os.path.exists(chemin):
        print(f"[ABSENT] {nom_fichier} - telechargement echoue, passage au fichier suivant")
        continue
    extraire_annees(chemin, type_fichier, ANNEES_CIBLES)

print("\n" + "=" * 60)
print("TERMINE")
print("=" * 60)