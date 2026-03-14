import requests
import os
import pandas as pd
import csv as csv_mod

RAW_DIR   = "data/raw/accidents_uk"
os.makedirs(RAW_DIR, exist_ok=True)

ANNEES_CIBLES = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021]
BASE_URL      = "https://data.dft.gov.uk/road-accidents-safety-data"
CHUNKSIZE     = 50_000

FICHIERS_COMPLETS = {
    "collision": "dft-road-casualty-statistics-collision-1979-latest-published-year.csv",
    "casualty":  "dft-road-casualty-statistics-casualty-1979-latest-published-year.csv",
    "vehicle":   "dft-road-casualty-statistics-vehicle-1979-latest-published-year.csv",
}

COLONNE_ANNEE = {
    "collision": "accident_year",
    "casualty":  "accident_year",
    "vehicle":   "accident_year",
}

# ------------------------------------------------------------
# Helpers
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


def find_year_col(columns, preferred):
    """Find year column — use preferred name or fall back to any col with 'year'."""
    if preferred in columns:
        return preferred
    candidates = [c for c in columns if "year" in c.lower()]
    if candidates:
        print(f"  ATTENTION : '{preferred}' introuvable, utilisation de '{candidates[0]}'")
        return candidates[0]
    return None


# ------------------------------------------------------------
# Core: chunked extraction
# Streams the large file, filters by year, writes one output
# file per year — never loads more than CHUNKSIZE rows at once.
# ------------------------------------------------------------

def extraire_annees_chunked(chemin_source, type_fichier, annees):
    print(f"\n[EXTRACTION] {type_fichier} — lecture par chunks...")

    encoding, sep = detect_params(chemin_source)
    print(f"  Format détecté : encoding={encoding}  sep={repr(sep)}")

    col_annee     = COLONNE_ANNEE[type_fichier]
    writers       = {}   # annee → output file handle
    header_written= {}   # annee → bool
    row_counts    = {}   # annee → int
    total_read    = 0

    # Output paths — one file per year
    out_paths = {
        annee: os.path.join(
            RAW_DIR,
            f"dft-road-casualty-statistics-{type_fichier}-{annee}.csv"
        )
        for annee in annees
    }

    # Skip years whose output already exists
    annees_todo = [a for a in annees if not os.path.exists(out_paths[a])]
    annees_skip = [a for a in annees if     os.path.exists(out_paths[a])]
    for a in annees_skip:
        print(f"  [SKIP] {os.path.basename(out_paths[a])} (déjà présent)")
    if not annees_todo:
        print("  Tous les fichiers déjà présents.")
        return

    # Open output files
    file_handles = {a: open(out_paths[a], "w", newline="", encoding="utf-8")
                    for a in annees_todo}
    header_written = {a: False for a in annees_todo}
    row_counts     = {a: 0     for a in annees_todo}

    try:
        for chunk in pd.read_csv(chemin_source, chunksize=CHUNKSIZE,
                                  low_memory=False, encoding=encoding,
                                  sep=sep, on_bad_lines="skip"):
            total_read += len(chunk)

            # Resolve year column on first chunk
            if col_annee not in chunk.columns:
                col_annee = find_year_col(chunk.columns, col_annee)
                if col_annee is None:
                    print("  ERREUR : impossible de trouver la colonne année. Abandon.")
                    return

            for annee in annees_todo:
                subset = chunk[chunk[col_annee] == annee]
                if len(subset) == 0:
                    continue
                subset.to_csv(
                    file_handles[annee],
                    header=not header_written[annee],
                    index=False
                )
                header_written[annee] = True
                row_counts[annee]    += len(subset)

            print(f"  ... {total_read:,} lignes lues", end="\r")

    finally:
        for fh in file_handles.values():
            fh.close()

    print(f"\n  Total lu : {total_read:,} lignes")
    for annee in annees_todo:
        n = row_counts[annee]
        if n > 0:
            print(f"  [OK]   {os.path.basename(out_paths[annee])} ({n:,} lignes)")
        else:
            # Remove empty file
            os.remove(out_paths[annee])
            print(f"  [VIDE] {annee} — aucune ligne trouvée, fichier supprimé")


# ------------------------------------------------------------
# Etape 1 : Telecharger les fichiers complets si absents
# ------------------------------------------------------------
print("=" * 60)
print("ETAPE 1 : Téléchargement des fichiers complets (1979-2024)")
print("=" * 60)

for type_fichier, nom_fichier in FICHIERS_COMPLETS.items():
    chemin = os.path.join(RAW_DIR, nom_fichier)
    if os.path.exists(chemin):
        print(f"[SKIP] {nom_fichier} (déjà présent)")
        continue
    url = f"{BASE_URL}/{nom_fichier}"
    telecharger_fichier(url, chemin)

# ------------------------------------------------------------
# Etape 2 : Extraire les annees cibles par chunks
# ------------------------------------------------------------
print("\n" + "=" * 60)
print(f"ETAPE 2 : Extraction des années {ANNEES_CIBLES}")
print("=" * 60)

for type_fichier, nom_fichier in FICHIERS_COMPLETS.items():
    chemin = os.path.join(RAW_DIR, nom_fichier)
    if not os.path.exists(chemin):
        print(f"[ABSENT] {nom_fichier} — téléchargement échoué, fichier ignoré")
        continue
    extraire_annees_chunked(chemin, type_fichier, ANNEES_CIBLES)

print("\n" + "=" * 60)
print("TERMINÉ")
print(f"Fichiers filtrés dans : {RAW_DIR}")
print("=" * 60)