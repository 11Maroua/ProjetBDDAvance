import requests
import os
import gzip
import shutil
import re
import pandas as pd

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
RAW_DIR = "data/raw/meteo_fr"
os.makedirs(RAW_DIR, exist_ok=True)

ANNEES_CIBLES = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021]
DATASET_ID    = "6569b27598256cc583c917a7"
ANNEE_MIN     = min(ANNEES_CIBLES)

# ------------------------------------------------------------
# Fonctions
# ------------------------------------------------------------
def lister_fichiers_dataset(dataset_id):
    print("[API] Recuperation de la liste des fichiers...")
    url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    ressources = r.json().get("resources", [])
    fichiers = []
    for res in ressources:
        if res.get("format", "").lower() in ("csv.gz", "gz", "csv"):
            fichiers.append({
                "titre":  res.get("title", ""),
                "url":    res.get("url", ""),
                "format": res.get("format", ""),
            })
    print(f"[API] {len(fichiers)} fichiers CSV/GZ trouves")
    return fichiers


def fichier_est_utile(nom_fichier):
    """Retourne True si le fichier peut contenir des donnees >= 2005."""
    annees = re.findall(r"\d{4}", nom_fichier)
    if not annees:
        return True
    annee_fin = int(annees[-1])
    return annee_fin >= ANNEE_MIN


def telecharger_fichier(url, chemin_dest):
    print(f"[DOWNLOAD] {os.path.basename(chemin_dest)}")
    try:
        r = requests.get(url, stream=True, timeout=300)
        r.raise_for_status()
        taille = 0
        with open(chemin_dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
                taille += len(chunk)
        print(f"           OK : {taille / 1024 / 1024:.1f} MB")
        return True
    except Exception as e:
        print(f"           ERREUR : {e}")
        return False


def decompresser_gz(chemin_gz, chemin_csv):
    print(f"[DECOMPRESS] {os.path.basename(chemin_gz)}")
    try:
        with gzip.open(chemin_gz, "rb") as f_in:
            with open(chemin_csv, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(chemin_gz)
        print(f"           OK")
        return True
    except Exception as e:
        print(f"           ERREUR : {e}")
        return False


def detecter_colonne_date(colonnes):
    """Trouve la colonne date parmi les colonnes disponibles."""
    priorite = ["DATE", "AAAAMMJJ", "DATE_OBS", "date", "aaaammjj"]
    for candidat in priorite:
        if candidat in colonnes:
            return candidat
    # Fallback : premiere colonne contenant 'date'
    for col in colonnes:
        if "date" in col.lower():
            return col
    return None


def parser_annee(serie):
    """
    Parse une serie de dates au format YYYYMMDD (entier ou string)
    et retourne une serie d'annees (int).
    """
    serie_str = serie.astype(str).str.strip()

    # Format YYYYMMDD : ex 20050115
    annees = pd.to_datetime(serie_str, format="%Y%m%d", errors="coerce").dt.year

    # Si echec, essayer format YYYY-MM-DD
    masque_null = annees.isna()
    if masque_null.any():
        annees_alt = pd.to_datetime(
            serie_str[masque_null], format="%Y-%m-%d", errors="coerce"
        ).dt.year
        annees.loc[masque_null] = annees_alt

    return annees


def extraire_annees_depuis_csv(chemin, annees_cibles, raw_dir):
    nom_csv = os.path.basename(chemin)

    if not fichier_est_utile(nom_csv):
        print(f"[IGNORE] {nom_csv} (anterieur a {ANNEE_MIN})")
        return

    print(f"\n[LECTURE] {nom_csv}")
    try:
        df = pd.read_csv(chemin, sep=";", low_memory=False)
    except Exception:
        try:
            df = pd.read_csv(chemin, sep=",", low_memory=False)
        except Exception as e:
            print(f"  ERREUR lecture : {e}")
            return

    print(f"  {len(df):,} lignes | colonnes : {list(df.columns)}")

    col_date = detecter_colonne_date(list(df.columns))
    if col_date is None:
        print("  ATTENTION : colonne date introuvable, passage au suivant")
        return

    print(f"  Colonne date utilisee : '{col_date}'")
    print(f"  Exemple de valeurs date : {df[col_date].head(3).tolist()}")

    df = df.copy()
    df.loc[:, "_annee"] = parser_annee(df[col_date])

    annees_presentes = sorted(
        df["_annee"].dropna().astype(int).unique().tolist()
    )
    print(f"  Annees presentes : {annees_presentes}")

    for annee in annees_cibles:
        if annee not in annees_presentes:
            continue
        nom_sortie    = f"meteo_fr_{annee}.csv"
        chemin_sortie = os.path.join(raw_dir, nom_sortie)
        df_annee      = df.loc[df["_annee"] == annee].drop(columns=["_annee"])

        if os.path.exists(chemin_sortie):
            df_annee.to_csv(chemin_sortie, mode="a", header=False, index=False)
            print(f"  [APPEND] {nom_sortie} (+{len(df_annee):,} lignes)")
        else:
            df_annee.to_csv(chemin_sortie, index=False)
            print(f"  [OK]     {nom_sortie} ({len(df_annee):,} lignes)")


# ------------------------------------------------------------
# Etape 1 : Telecharger uniquement les fichiers utiles
# ------------------------------------------------------------
print("=" * 60)
print("ETAPE 1 : Telechargement des fichiers meteo FR (SIM quotidienne)")
print("=" * 60)

fichiers = lister_fichiers_dataset(DATASET_ID)

for fic in fichiers:
    nom_base = os.path.basename(fic["url"]).split("?")[0]
    nom_gz   = nom_base if nom_base.endswith(".gz") else nom_base + ".gz"
    nom_csv  = nom_gz.replace(".gz", "")

    if not fichier_est_utile(nom_gz):
        print(f"[IGNORE] {nom_gz} (anterieur a {ANNEE_MIN})")
        continue

    chemin_gz  = os.path.join(RAW_DIR, nom_gz)
    chemin_csv = os.path.join(RAW_DIR, nom_csv)

    if os.path.exists(chemin_csv):
        print(f"[SKIP] {nom_csv} (deja present)")
        continue

    if telecharger_fichier(fic["url"], chemin_gz):
        decompresser_gz(chemin_gz, chemin_csv)

# ------------------------------------------------------------
# Etape 2 : Extraction des annees cibles
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("ETAPE 2 : Extraction des annees", ANNEES_CIBLES)
print("=" * 60)

csv_bruts = sorted([
    f for f in os.listdir(RAW_DIR)
    if f.endswith(".csv")
    and not f.startswith("meteo_fr_")
])

print(f"Fichiers a traiter : {csv_bruts}")

for nom in csv_bruts:
    chemin = os.path.join(RAW_DIR, nom)
    extraire_annees_depuis_csv(chemin, ANNEES_CIBLES, RAW_DIR)

print("\n" + "=" * 60)
print("TERMINE")
print("=" * 60)