# Entrepôt de données — Accidents de la route et Météo (France / Royaume-Uni)

Projet de bases de données avancées — construction d'un entrepôt de données en schéma étoile croisant les accidents de la route et les données météorologiques en France et au Royaume-Uni sur les années 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019 et 2021.

---

## Prérequis
```bash
pip install requests pandas pyproj holidays sqlalchemy psycopg2-binary
```

- Python 3.9+
- PostgreSQL 14+

---

## Structure du projet


---

## Sources de données

| Source | Pays | Licence |
|--------|------|---------|
| https://www.data.gouv.fr/datasets/donnees-changement-climatique-sim-quotidienne | FR | Licence Ouverte v2.0 |
| https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2022 | FR | Licence Ouverte v2.0 |
|https://www.gov.uk/government/statistical-data-sets/road-safety-open-data | UK | OGL v3.0 |
| https://www.kaggle.com/datasets/robjbutlermei/uk-daily-weather-1961-2024 | UK | CC0 |

---

## Étape 1 — Télécharger les données brutes

Les données brutes ne sont pas sur GitHub car trop volumineuses (~6 GB au total).  
Lancer les 4 scripts dans l'ordre :
```bash
python3 src/scripts_recup_donnees/download_accidents_fr.py
python3 src/scripts_recup_donnees/download_accidents_uk.py
python3 src/scripts_recup_donnees/download_meteo_fr.py
python3 src/scripts_recup_donnees/download_meteo_uk.py
```

Les fichiers sont téléchargés dans `data/raw/`.

> **Kaggle** : `download_meteo_uk.py` nécessite un fichier `~/.kaggle/kaggle.json` :
> ```json
> {"username": "votre_username", "key": "votre_api_key"}
> ```

---

## Étape 2 — Générer DIM_TEMPS et DIM_PAYS

Ouvrir et exécuter toutes les cellules du notebook :
```
src/scripts_creation_fact_table/generation_dimTemps.ipynb
```

Ajouter cette cellule en première position avant d'exécuter :
```python
import os
os.chdir("/chemin/absolu/vers/ProjetBDDAvance")
```

Produit dans `data/processed/` :
- `DIM_TEMPS.csv` — 3 285 lignes, une par jour pour les 9 années
- `DIM_PAYS.csv` — 2 lignes : France (id=1) et Royaume-Uni (id=2)

---

## Étape 3 — Générer DIM_METEO
```bash
python3 src/scripts_creation_fact_table/preprocess_meteo.py
```

Produit `data/processed/dim_meteo.csv` — une ligne par jour par pays.

---

## Étape 4 — Finaliser la construction des autres dimensions et la table des faits
```bash
python3 src/scripts_creation_fact_table/buildfait.py
```

Lit tous les fichiers de `data/raw/` et `data/processed/` et produit dans `data/dims/` :

| Fichier | Contenu |
|---------|---------|
| `dim_pays.csv` | 2 lignes |
| `dim_date.csv` | Calendrier des 9 années |
| `dim_meteo.csv` | Météo quotidienne FR + UK |
| `dim_localisation.csv` | Lieux des accidents |
| `dim_usager.csv` | Usagers impliqués |
| `dim_vehicule.csv` | Véhicules impliqués |
| `fait_accident.csv` | Table des faits |


---

## Étape 5 — Créer le schéma PostgreSQL
```bash
psql -U postgres -d accidents_db -f schema_accidents.sql
```

---

## Étape 6 — Charger les données dans PostgreSQL

Éditer `DB_URL` dans `loaddb.py` :
```python
DB_URL = "postgresql://postgres:monmotdepasse@localhost:5432/accidents_db"
```

Puis lancer :
```bash
python3 src/scripts_creation_fact_table/loaddb.py
```

---

## Documentation

`docs/guide_colonnesSources_vers_Dimensions.md` — détail du mapping entre les colonnes des fichiers bruts et les colonnes des dimensions, avec les correspondances FR ↔ UK et les points d'attention par champ.

`docs/guide_generation_table_fait.md` — détail du fonctionnement de `buildfait.py` : décodage des codes numériques, conversion des coordonnées, construction des clés surrogates et calcul des mesures de la table des faits.