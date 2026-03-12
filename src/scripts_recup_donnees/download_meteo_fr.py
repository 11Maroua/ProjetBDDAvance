import requests
import os
import gzip
import re
import csv

RAW_DIR = "data/raw/meteo_fr"
os.makedirs(RAW_DIR, exist_ok=True)

ANNEES_CIBLES = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021]
DATASET_ID = "6569b27598256cc583c917a7"
ANNEE_MIN = min(ANNEES_CIBLES)


def lister_fichiers_dataset(dataset_id):
    print("[API] Recuperation de la liste des fichiers...")
    url = "https://www.data.gouv.fr/api/1/datasets/" + dataset_id + "/"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    ressources = r.json().get("resources", [])

    fichiers = []
    for res in ressources:
        fmt = res.get("format", "").lower()
        if fmt in ("csv.gz", "gz", "csv"):
            fichiers.append({
                "titre": res.get("title", ""),
                "url": res.get("url", ""),
                "format": fmt,
            })

    print(f"[API] {len(fichiers)} fichiers CSV/GZ trouves")
    return fichiers


def fichier_est_utile(nom_fichier):
    annees = re.findall(r"\d{4}", nom_fichier)
    if not annees:
        return True
    annee_fin = int(annees[-1])
    return annee_fin >= ANNEE_MIN


def telecharger_fichier(url, chemin_dest):
    if os.path.exists(chemin_dest) and os.path.getsize(chemin_dest) > 0:
        print(f"[SKIP] {os.path.basename(chemin_dest)} deja present")
        return True

    print(f"[DOWNLOAD] {os.path.basename(chemin_dest)}")
    try:
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            taille = 0
            with open(chemin_dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        taille += len(chunk)
                        if taille % (100 * 1024 * 1024) < 1024 * 1024:
                            print(f"           {taille / 1024 / 1024:.0f} MB telecharges...")
        print(f"           OK : {taille / 1024 / 1024:.1f} MB")
        return True
    except Exception as e:
        print(f"           ERREUR : {e}")
        return False


def detecter_delimiteur_et_colonnes(chemin_gz):
    with gzip.open(chemin_gz, "rt", encoding="utf-8", errors="replace", newline="") as f:
        echantillon = f.read(8192)
        f.seek(0)

        try:
            dialect = csv.Sniffer().sniff(echantillon, delimiters=";,")
            delim = dialect.delimiter
        except Exception:
            delim = ";"

        reader = csv.reader(f, delimiter=delim)
        header = next(reader)

    return delim, header


def detecter_colonne_date(colonnes):
    priorite = ["DATE", "AAAAMMJJ", "DATE_OBS", "date", "aaaammjj"]
    for candidat in priorite:
        if candidat in colonnes:
            return candidat

    for col in colonnes:
        if "date" in col.lower():
            return col

    return None


def parser_annee(valeur):
    if valeur is None:
        return None

    s = str(valeur).strip()
    if not s:
        return None

    # 20050115
    m = re.match(r"^(\d{4})\d{4}$", s)
    if m:
        return int(m.group(1))

    # 2005-01-15
    m = re.match(r"^(\d{4})-\d{2}-\d{2}$", s)
    if m:
        return int(m.group(1))

    # fallback : premier bloc 4 chiffres
    m = re.search(r"(\d{4})", s)
    if m:
        return int(m.group(1))

    return None


def extraire_annees_depuis_gz(chemin_gz, annees_cibles, raw_dir):
    nom_gz = os.path.basename(chemin_gz)

    if not fichier_est_utile(nom_gz):
        print(f"[IGNORE] {nom_gz} (anterieur a {ANNEE_MIN})")
        return

    print(f"\n[LECTURE STREAM] {nom_gz}")

    delim, header = detecter_delimiteur_et_colonnes(chemin_gz)
    print(f"  Delimiteur detecte : '{delim}'")
    print(f"  Colonnes detectees : {header}")

    col_date = detecter_colonne_date(header)
    if col_date is None:
        print("  ATTENTION : colonne date introuvable")
        return

    print(f"  Colonne date utilisee : {col_date}")

    writers = {}
    files_out = {}
    nb_lignes = 0
    nb_gardees = {annee: 0 for annee in annees_cibles}

    try:
        with gzip.open(chemin_gz, "rt", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f, delimiter=delim)

            for row in reader:
                nb_lignes += 1
                annee = parser_annee(row.get(col_date))

                if annee not in annees_cibles:
                    continue

                chemin_sortie = os.path.join(raw_dir, f"meteo_fr_{annee}.csv")

                if annee not in writers:
                    existe_deja = os.path.exists(chemin_sortie) and os.path.getsize(chemin_sortie) > 0
                    mode = "a" if existe_deja else "w"
                    fout = open(chemin_sortie, mode, encoding="utf-8", newline="")
                    writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
                    if not existe_deja:
                        writer.writeheader()

                    files_out[annee] = fout
                    writers[annee] = writer

                writers[annee].writerow(row)
                nb_gardees[annee] += 1

                if nb_lignes % 500000 == 0:
                    print(f"  ... {nb_lignes:,} lignes lues")

    finally:
        for fout in files_out.values():
            fout.close()

    print(f"  Total lignes lues : {nb_lignes:,}")
    for annee in annees_cibles:
        if nb_gardees[annee] > 0:
            print(f"  [OK] meteo_fr_{annee}.csv : {nb_gardees[annee]:,} lignes")


print("=" * 60)
print("ETAPE 1 : Telechargement des fichiers meteo FR")
print("=" * 60)

fichiers = lister_fichiers_dataset(DATASET_ID)

for fic in fichiers:
    nom_base = os.path.basename(fic["url"]).split("?")[0]
    nom_gz = nom_base if nom_base.endswith(".gz") else nom_base + ".gz"

    if not fichier_est_utile(nom_gz):
        print(f"[IGNORE] {nom_gz} (anterieur a {ANNEE_MIN})")
        continue

    chemin_gz = os.path.join(RAW_DIR, nom_gz)
    telecharger_fichier(fic["url"], chemin_gz)

print("\n" + "=" * 60)
print("ETAPE 2 : Extraction des annees cibles en streaming")
print("=" * 60)

fichiers_gz = sorted(
    f for f in os.listdir(RAW_DIR)
    if f.endswith(".gz")
)

print(f"Fichiers a traiter : {fichiers_gz}")

for nom in fichiers_gz:
    chemin = os.path.join(RAW_DIR, nom)
    if os.path.getsize(chemin) == 0:
        print(f"[IGNORE] {nom} (fichier vide)")
        continue
    extraire_annees_depuis_gz(chemin, ANNEES_CIBLES, RAW_DIR)

print("\n" + "=" * 60)
print("TERMINE")
print("=" * 60)