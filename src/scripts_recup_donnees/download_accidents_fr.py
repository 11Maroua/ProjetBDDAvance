import requests
import os

# Dossier de destination
os.makedirs("data/raw/accidents_fr", exist_ok=True)

# Années qu'on veut (1 sur 2 depuis 2005)
ANNEES = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021]

# Types de fichiers par année
TYPES = ["caracteristiques", "lieux", "vehicules", "usagers"]

# L'API data.gouv.fr pour ce dataset
DATASET_ID = "53698f4ca3a729239d2036df"

print("Récupération de la liste des fichiers...")
url = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ID}/"
response = requests.get(url)
data = response.json()

# Liste tous les fichiers disponibles
resources = data["resources"]
print(f"{len(resources)} fichiers trouvés au total\n")

# Filtrer et télécharger uniquement ce qu'on veut
telecharges = 0
for resource in resources:
    nom = resource["title"].lower()
    url_fichier = resource["url"]
    
    # Vérifier si c'est une année qui nous intéresse
    annee_trouvee = None
    for annee in ANNEES:
        if str(annee) in nom:
            annee_trouvee = annee
            break
    
    if annee_trouvee is None:
        continue
    
    # Vérifier si c'est un des 4 types de fichiers
    type_trouve = None
    for t in TYPES:
        if t in nom:
            type_trouve = t
            break
    
    if type_trouve is None:
        continue
    
    # Télécharger
    nom_fichier = f"{type_trouve}_{annee_trouvee}.csv"
    chemin = f"data/raw/accidents_fr/{nom_fichier}"
    
    if os.path.exists(chemin):
        print(f"Déjà téléchargé : {nom_fichier}")
        continue
    
    print(f"Téléchargement : {nom_fichier}...")
    r = requests.get(url_fichier, stream=True)
    with open(chemin, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Sauvegardé : {nom_fichier}")
    telecharges += 1

print(f"\nTerminé ! {telecharges} fichiers téléchargés.")