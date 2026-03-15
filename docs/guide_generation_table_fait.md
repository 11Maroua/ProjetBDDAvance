## Étape 4 — Construire les dimensions et la table des faits
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

 
### Ce que fait ce script 
Le script traite les 9 années une par une, d'abord FR puis UK. Pour chaque année il construit les dimensions et la table des faits en un seul passage.

**Lecture des fichiers**

Chaque fichier CSV est lu avec détection automatique de l'encodage (`utf-8`, `latin-1`) et du séparateur (`,` `;` `\t`). Les espaces insécables `\xa0` sont supprimés dès la lecture — ils causaient des erreurs silencieuses dans les jointures sur `Num_Acc`.

**Décodage des codes numériques**

Les fichiers bruts stockent des codes numériques. Le script les convertit en texte lisible via des tables de décodage :

| Code source | Valeur brute | Valeur décodée |
|-------------|-------------|----------------|
| FR `grav` | 2 | `"Killed"` |
| FR `grav` | 3 | `"Seriously injured"` |
| FR `grav` | 4 | `"Slightly injured"` |
| UK `casualty_severity` | 1 | `"Killed"` |
| FR `sexe` | 1 | `"Male"` |
| FR `catv` | 7 | `"Car"` |
| FR `motor` | 3 | `"Electric"` |

**Coordonnées géographiques**

Les coordonnées FR sont parfois en Lambert93 (système cartographique français) au lieu de GPS standard. Le script détecte automatiquement le système : si `abs(latitude) > 90` c'est du Lambert93, sinon c'est déjà du WGS84. La conversion est faite via la librairie `pyproj`.

Les vitesses UK sont en mph — converties en km/h dans `dim_localisation`.

**Construction des clés surrogates**

Les fichiers bruts utilisent `Num_Acc` (FR) et `collision_index` (UK) comme identifiants. Le script crée ses propres identifiants numériques séquentiels (`id_lieu`, `id_usager`, `id_vehicule`, `id_accident`) via des compteurs globaux qui s'incrémentent à travers toutes les années et les deux pays — garantissant l'unicité dans toute la base.

**Mapping Num_Acc → ids**

Pour chaque année, le script construit des dictionnaires de correspondance :
```
loc_map    = { Num_Acc → id_lieu    }
usager_map = { Num_Acc → id_usager  }
veh_map    = { Num_Acc → id_vehicule}
```
Ces dictionnaires permettent d'assembler FAIT_ACCIDENT en récupérant les bons ids sans refaire les calculs.

**Calcul des mesures**

Pour chaque accident, les gravités de tous les usagers impliqués sont agrégées :
- `nb_tues` — nombre d'usagers avec `gravite = "Killed"`
- `nb_blesses_graves` — nombre d'usagers avec `gravite = "Seriously injured"`
- `nb_blesses_legers` — nombre d'usagers avec `gravite = "Slightly injured"`
- `nb_victimes_total` — somme des trois
- `indice_gravite` — `nb_tues × 3 + nb_blesses_graves × 2 + nb_blesses_legers × 1`


Les fichiers CSV de sortie restent ouverts pendant tout le traitement et sont alimentés année par année. Cela évite de charger toutes les années en mémoire simultanément et surcharger la RAM.