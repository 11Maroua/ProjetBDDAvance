import requests
import os

os.makedirs("data/raw/accidents_fr", exist_ok=True)

ANNEES = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021]
TYPES = ["caracteristiques", "lieux", "vehicules", "usagers"]
DATASET_ID = "53698f4ca3a729239d2036df"

# Mapping pour normaliser les noms de types 
# Gere les variantes dans l'api : "caract", "carcteristiques", "caracteristiques", "Caract_"
def normaliser_type(nom):
    nom = nom.lower()
    if "caract" in nom or "carct" in nom:  # gere la presence d'une faute de frappe data.gouv
        return "caracteristiques"
    if "lieu" in nom:
        return "lieux"
    if "vehic" and "imm" in nom:
        return "vehicules_immatricules"
    if "vehicules_" in nom: 
        return "vehicules"
    if "vehicules-2" in nom: 
        return "vehicules"
    if "usag" in nom:
        return "usagers"
    return None
print("Recuperation de la liste des fichiers...")
url = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ID}/"
response = requests.get(url)
data = response.json()
resources = data["resources"]
print(f"{len(resources)} fichiers trouves au total\n")

telecharges = 0
for resource in resources:
    nom    = resource["title"]
    url_f  = resource["url"]

    # Identifier l'annee
    annee_trouvee = None
    for annee in ANNEES:
        if str(annee) in nom:
            annee_trouvee = annee
            break
    if annee_trouvee is None:
        continue

    # Identifier le type avec normalisation
    type_trouve = normaliser_type(nom)
    if type_trouve is None:
        continue

    # Nom de fichier normalise et coherent
    nom_fichier = f"{type_trouve}_{annee_trouvee}.csv"
    chemin = f"data/raw/accidents_fr/{nom_fichier}"

    if os.path.exists(chemin):
        print(f"[SKIP] {nom_fichier}")
        continue

    print(f"[DOWNLOAD] {nom} -> {nom_fichier}")
    r = requests.get(url_f, stream=True)
    with open(chemin, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"[OK] {nom_fichier}")
    telecharges += 1

print(f"\nTermine : {telecharges} fichiers telecharges.")