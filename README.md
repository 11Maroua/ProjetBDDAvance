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
```
ProjetBDDAvance/
├── data/
│   ├── dims/          ← dimensions finales (ignorées par git à générer avec scripts création fact_table)
│   └── raw/           ← données brutes (ignorées par git à générer avec scripts création recup_données)
├── docs/
│   ├── guides/
│   │   ├── guide_colonnesSources_vers_Dimensions.md
│   │   └── guide_generation_table_fait.md
│   └── images/
│       └── schema_etoile.png
├── src/
│   ├── control_access/
│   │   ├── controles_acces.sql
│   │   └── tests_rls.txt
│   ├── materialized_views/
│   │   ├── analyze_impact_mv.md
│   │   ├── explain_analyze_results.txt
│   │   ├── mv_requests.sql
│   │   └── mv.sql
│   ├── queries/
│   │   ├── queries_Adele/
│   │   ├── queries_Ikram/
│   │   ├── queries_Lila/
│   │   └── queries_Maroua/
│   ├── scripts_creation_fact_table/
│   │   ├── build_entrepot.py
│   │   ├── build_pays
│   │   ├── build_temps
│   │   ├── build_usager_vehicule_localisation_fait.py
│   │   ├── loaddb.py
│   │   └── preprocess_meteo.py
│   └── scripts_recup_donnees/
│       ├── download_accidents_fr.py
│       ├── download_accidents_uk.py
│       ├── download_meteo_fr.py
│       └── download_meteo_uk.py
├── .gitignore
├── README.md
├── schema_accidents.sql
```
---
## Schéma de l'entrepot

![Schéma étoile](docs/images/schema_etoile.jpg)

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

## Étape 2 — Générer les fichiers de l'entrepôt

Exécuter le script suivant pour générer toutes les dimensions ainsi que la table des faits. 

```bash
python3 src/scripts_creation_fact_table/build_entrepot.py
```

Lit tous les fichiers de `data/raw/` et produit dans `data/dims/` :

| Fichier | Contenu |
|---------|---------|
| `dim_pays.csv` | 2 lignes |
| `dim_temps.csv` | Calendrier des 9 années |
| `dim_meteo.csv` | Météo quotidienne FR + UK |
| `dim_localisation.csv` | Lieux des accidents |
| `dim_usager.csv` | Usagers impliqués |
| `dim_vehicule.csv` | Véhicules impliqués |
| `fait_accident.csv` | Table des faits |

---

## Étape 3 — Créer la base de données
```bash
psql -U postgres -c "CREATE DATABASE accidents_db;"
```

---

## Étape 4 — Créer le schéma PostgreSQL
```bash
psql -U postgres -d accidents_db -f schema_accidents.sql
```

---

## Étape 5 — Charger les données dans PostgreSQL

Éditer `DB_URL` dans `loaddb.py` :
```python
DB_URL = "postgresql://postgres:monmotdepasse@localhost:5432/accidents_db"
```

Puis lancer :
```bash
python3 src/scripts_creation_fact_table/loaddb.py
```

---

## Étape 6 — interroger notre entrepot 
```bash
psql -U postgres -d accidents_db 
```
## Étape 7 — Indexation et optimisation
```bash
psql -U postgres -d accidents_db -f src/indexes/indexes.sql
```
Les index créés accélèrent les jointures sur `FAIT_ACCIDENT` et `DIM_TEMPS`.
Voir `src/indexes/analyze_impact_indexes.md` pour l'analyse complète de l'impact

## Étape 8 — Vues matérialisées
```bash
psql -U postgres -d accidents_db -f src/materialized_views/mv.sql
```
Voir `src/materialized_viexs/analyze_impact_mv.md` pour l'analyse complète de l'impact. 

## Étape 9 - Contrôle d'accès

Chaque rôle a accès aux différentes tables selon le tableau suivant.
De plus nous avons activé le Row Security Level sur toutes les tables de notre entrpôt et utilisé une fonction pour faire en sorte que chaque personne accédant à l'entrepôt n'ai accès qu'aux lignes correspondant à son pays d'origine.

| Table | ADMIN_USER | MEDECIN | POLICIER | ADMIN_GLOBAL |
|---|---|---|---|---|
| `FAIT_ACCIDENT` | SELECT | SELECT | SELECT | Tout |
| `DIM_LOCALISATION` | X | SELECT | SELECT | Tout |
| `DIM_USAGER` | X | SELECT | SELECT | Tout |
| `DIM_VEHICULE` | X  | X | SELECT | Tout |
| `DIM_TEMPS` | X | SELECT | SELECT | Tout |
| `DIM_PAYS` | SELECT | SELECT | SELECT | Tout |
| `DIM_METEO` | X | SELECT | SELECT | Tout |
| `UTILISATEUR_PAYS` | Ligne propre | Ligne propre | Ligne propre | Tout |


## Mise à jour de l'entrepôt

La mise à jour de l'entrepôt suit un processus **incrémental** — on n'insère que les nouveaux enregistrements sans toucher aux données existantes.

### Détection des nouveaux enregistrements

Les clés des sources permettent de détecter ce qui est nouveau :
- France : colonne `Num_Acc` dans les fichiers accidents FR
- Royaume-Uni : colonne `collision_index` dans les fichiers accidents UK

### Procédure

**1. Télécharger les nouvelles données sources**
```bash
python3 src/scripts_recup_donnees/download_accidents_fr.py
python3 src/scripts_recup_donnees/download_accidents_uk.py
python3 src/scripts_recup_donnees/download_meteo_fr.py
python3 src/scripts_recup_donnees/download_meteo_uk.py
```

**2. Regénérer les fichiers CSV**
```bash
python3 src/scripts_creation_fact_table/build_entrepot.py
```

**3. Insérer dans PostgreSQL**

`loaddb.py` utilise `if_exists="append"` — les données existantes ne sont jamais écrasées. Les doublons sont gérés par la contrainte `ON CONFLICT DO NOTHING` sur les clés primaires.

```bash
python3 src/scripts_creation_fact_table/loaddb.py
```

---

## Documentation

`docs/guide_colonnesSources_vers_Dimensions.md` — détail du mapping entre les colonnes des fichiers bruts et les colonnes des dimensions, avec les correspondances FR ↔ UK et les points d'attention par champ.

`docs/guide_generation_table_fait.md` — détail du fonctionnement de `buildfait.py` : décodage des codes numériques, conversion des coordonnées, construction des clés surrogates et calcul des mesures de la table des faits.

---

## Licence

Ce projet est distribué sous **Licence Ouverte / Open Licence Version 2.0** .
Toute réutilisation doit mentionner les sources originales listées dans la section **Sources de données**.