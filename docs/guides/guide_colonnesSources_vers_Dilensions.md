# Guide du passage des colonnes des données SOURCES vers les colonnes des données DIMENSIONS

## Comment utiliser ce document

Ce fichier explique quelles colonnes des fichiers bruts (`data/raw/`) alimentent quelles dimensions et la table de faits. Les fichiers bruts ne sont pas sur GitHub (car trop volumineux) — relancer les scripts dans `src/scripts_recup_donnees/` pour les régénérer.

Les dimensions temps et pays déjà produites sont dans `data/processed/` et directement utilisables :

```python
dim_temps = pd.read_csv("data/processed/DIM_TEMPS.csv")
dim_pays  = pd.read_csv("data/processed/DIM_PAYS.csv")
```

---

## Sources disponibles

| Fichier | Localisation | Lignes (2021) | Colonnes |
|--------|-------------|---------------|----------|
| `caracteristiques_YYYY.csv` | `data/raw/accidents_fr/` | ~70 000 | 15 |
| `lieux_YYYY.csv` | `data/raw/accidents_fr/` | ~70 000 | 18 |
| `usagers_YYYY.csv` | `data/raw/accidents_fr/` | ~130 000 | 16 |
| `vehicules_YYYY.csv` | `data/raw/accidents_fr/` | ~120 000 | 11 |
| `dft-road-casualty-statistics-collision-YYYY.csv` | `data/raw/accidents_uk/` | ~100 000 | 44 |
| `dft-road-casualty-statistics-casualty-YYYY.csv` | `data/raw/accidents_uk/` | ~130 000 | 23 |
| `dft-road-casualty-statistics-vehicle-YYYY.csv` | `data/raw/accidents_uk/` | ~175 000 | 32 |
| `meteo_fr_YYYY.csv` | `data/raw/meteo_fr/` | ~3 600 000 | 29 |
| `meteo_uk_YYYY.csv` | `data/raw/meteo_uk/` | 365 | 10 |

---

## DIM_PAYS 

Fichier : `data/processed/DIM_PAYS.csv`

| id_pays | code_pays | nom_pays |
|---------|-----------|----------|
| 1 | FR | France |
| 2 | UK | Royaume-Uni |

> Créée manuellement, pas extraite des sources.

---

## DIM_TEMPS  

Fichier : `data/processed/DIM_TEMPS.csv`

| Colonne DIM_TEMPS | Source FR | Source UK | Notes |
|-------------------|-----------|-----------|-------|
| `date` | `jour` + `mois` + `an` (caracteristiques) | `date` (collision) | Format final : YYYY-MM-DD |
| `jour` | `jour` | extrait de `date` | |
| `mois` | `mois` | extrait de `date` | |
| `annee` | `an` | `collision_year` | |
| `trimestre` | calculé | calculé | |
| `jour_semaine` | calculé | `day_of_week` | 1=lundi, 7=dimanche |
| `nom_jour` | calculé | calculé | En français |
| `nom_mois` | calculé | calculé | En français |
| `est_weekend` | calculé | calculé | 1 si samedi ou dimanche |
| `est_ferie_fr` | calculé | — | librairie `holidays` |
| `est_ferie_uk` | — | calculé | librairie `holidays` |
| `saison` | calculé | calculé | Hiver/Printemps/Eté/Automne |

> DIM_TEMPS couvre toutes les dates des 9 années, une ligne par jour (3285 lignes total).  
> La jointure avec FAIT_ACCIDENT se fait sur la colonne `date`.

---

## DIM_LOCALISATION 

Sources à utiliser : `lieux_YYYY.csv` (FR) + `collision_YYYY.csv` (UK)

| Colonne suggérée | Source FR | Colonne FR | Source UK | Colonne UK | Notes |
|-----------------|-----------|------------|-----------|------------|-------|
| `id_pays` | — | — | — | — | 1=FR, 2=UK (jointure DIM_PAYS) |
| `departement` | lieux | `dep` (via caracteristiques) | — | — | Spécifique FR |
| `commune` | lieux | `com` (via caracteristiques) | — | — | Code INSEE |
| `latitude` | caracteristiques | `lat` | collision | `latitude` | Virgule → point pour FR |
| `longitude` | caracteristiques | `long` | collision | `longitude` | |
| `milieu` | lieux | `agg` (1=hors agglo, 2=agglo) | collision | `urban_or_rural_area` (1=urban, 2=rural) | |
| `type_route` | lieux | `catr` (1=autoroute…6=parc) | collision | `first_road_class` | Codes différents à harmoniser |
| `vitesse_limite` | lieux | `vma` | collision | `speed_limit` | km/h pour FR, mph pour UK → convertir |
| `type_intersection` | lieux | `int` (via caracteristiques) | collision | `junction_detail` | |
| `profil_route` | lieux | `prof` (1=plat, 2=pente…) | — | — | Spécifique FR |
| `surface_route` | lieux | `surf` | collision | `road_surface_conditions` | |

> Point d'attention : `lat` et `long` dans FR utilisent la virgule comme séparateur décimal → remplacer par un point avant conversion en float.  
> Point d'attention : Les vitesses UK sont en mph → multiplier par 1.60934 pour convertir en km/h.

---

## DIM_USAGER 

Source : `usagers_YYYY.csv` (FR) + `casualty_YYYY.csv` (UK)

| Colonne suggérée | Source FR | Colonne FR | Source UK | Colonne UK | Notes |
|-----------------|-----------|------------|-----------|------------|-------|
| `categorie_usager` | usagers | `catu` (1=conducteur, 2=passager, 3=piéton) | casualty | `casualty_class` (1=conducteur, 2=passager, 3=piéton) | Codes identiques |
| `gravite` | usagers | `grav` (1=indemne, 2=tué, 3=blessé hosp., 4=blessé léger) | casualty | `casualty_severity` (1=fatal, 2=grave, 3=léger) | Codes différents à harmoniser |
| `sexe` | usagers | `sexe` (1=M, 2=F) | casualty | `sex_of_casualty` (1=M, 2=F) | Codes identiques |
| `annee_naissance` | usagers | `an_nais` | — | — | Spécifique FR |
| `age` | — | — | casualty | `age_of_casualty` | Spécifique UK |
| `tranche_age` | — | — | casualty | `age_band_of_casualty` | Spécifique UK |
| `type_trajet` | usagers | `trajet` (1=domicile-travail…9=autre) | — | — | Spécifique FR |
| `equipement_securite` | usagers | `secu1`, `secu2`, `secu3` | — | — | Spécifique FR |

---

## DIM_VEHICULE 

Source : `vehicules_YYYY.csv` (FR) + `vehicle_YYYY.csv` (UK)

| Colonne suggérée | Source FR | Colonne FR | Source UK | Colonne UK | Notes |
|-----------------|-----------|------------|-----------|------------|-------|
| `categorie_vehicule` | vehicules | `catv` (1=bicyclette…99=autre) | vehicle | `vehicle_type` | Codes différents à harmoniser |
| `motorisation` | vehicules | `motor` (1=hydrocarbures…5=électrique) | vehicle | `propulsion_code` | |
| `manoeuvre` | vehicules | `manv` | vehicle | `vehicle_manoeuvre` | |
| `obstacle_heurte` | vehicules | `obs` + `obsm` | vehicle | `hit_object_in_carriageway` | |
| `age_vehicule` | — | — | vehicle | `age_of_vehicle` | Spécifique UK |
| `marque_modele` | — | — | vehicle | `generic_make_model` | Spécifique UK |
| `sexe_conducteur` | — | — | vehicle | `sex_of_driver` | Spécifique UK |
| `age_conducteur` | — | — | vehicle | `age_of_driver` | Spécifique UK |

---

## DIM_METEO

Source : `meteo_fr_YYYY.csv` + `meteo_uk_YYYY.csv`

| Colonne suggérée | Source FR | Colonne FR | Source UK | Colonne UK | Notes |
|-----------------|-----------|------------|-----------|------------|-------|
| `date` | meteo_fr | `DATE` (YYYYMMDD) | meteo_uk | `date` (YYYY-MM-DD) | Clé de jointure |
| `id_pays` | — | — | — | — | 1=FR, 2=UK |
| `temperature` | meteo_fr | `T` (°C) | meteo_uk | `temp` (°C) | |
| `precipitation` | meteo_fr | `PRELIQ` (mm, pluie liquide) | meteo_uk | `precipitation` (mm) | |
| `precipitation_neige` | meteo_fr | `PRENEI` (mm) | — | — | Spécifique FR |
| `vent` | meteo_fr | `FF` (m/s) | meteo_uk | `wind_speed` (m/s) | |
| `humidite` | meteo_fr | `HU` (%) | — | — | Spécifique FR |
| `enneigement` | meteo_fr | `HTEURNEIGE` (m) | — | — | Spécifique FR |

>  Météo FR : granularité par point de grille (LAMBX/LAMBY) → agréger par jour (moyenne nationale ou par département).  
>  Météo UK : déjà agrégée au niveau national, 1 ligne par jour.

---

## FAIT_ACCIDENT 

Clé primaire : `id_accident`

| Colonne | Source FR | Source UK | Notes |
|---------|-----------|-----------|-------|
| `id_accident` | `Num_Acc` | `collision_index` | Clé naturelle |
| `id_temps` | jointure sur `date` → DIM_TEMPS | jointure sur `date` → DIM_TEMPS | |
| `id_lieu` | jointure → DIM_LOCALISATION | jointure → DIM_LOCALISATION | |
| `id_usager` | jointure → DIM_USAGER | jointure → DIM_USAGER | |
| `id_vehicule` | jointure → DIM_VEHICULE | jointure → DIM_VEHICULE | |
| `id_meteo` | jointure sur `date` + `id_pays` → DIM_METEO | idem | |
| `id_pays` | 1 | 2 | |
| `nb_tues` | usagers : count(`grav`==2) | casualty : count(`casualty_severity`==1) | |
| `nb_blesses_graves` | usagers : count(`grav`==3) | casualty : count(`casualty_severity`==2) | |
| `nb_blesses_legers` | usagers : count(`grav`==4) | casualty : count(`casualty_severity`==3) | |
| `nb_victimes_total` | calculé | `number_of_casualties` (collision) | |
| `nb_vehicules` | calculé | `number_of_vehicles` (collision) | |
| `indice_gravite` | calculé : tués×3 + graves×2 + légers×1 | idem | Pondération à valider avec la prof |

---

## Clé de jointure entre les fichiers FR

Les 4 fichiers FR se rejoignent tous sur `Num_Acc` :

```
caracteristiques_YYYY.csv  ─┐
lieux_YYYY.csv             ─┤─ Num_Acc ─→ 1 accident
usagers_YYYY.csv           ─┤
vehicules_YYYY.csv         ─┘
```

## Clé de jointure entre les fichiers UK

Les 3 fichiers UK se rejoignent sur `collision_index` :

```
collision_YYYY.csv  ─┐
casualty_YYYY.csv   ─┤─ collision_index ─→ 1 accident
vehicle_YYYY.csv    ─┘
```

---

## Renseignement sur les colonnes calculées

Ces colonnes n'existent pas dans les sources brutes — elles sont calculées par `buildfait.py`.

### `indice_gravite` (FAIT_ACCIDENT)

Pondération de la gravité d'un accident basée sur le nombre de victimes :
```
indice_gravite = nb_tues × 3 + nb_blesses_graves × 2 + nb_blesses_legers × 1
```

| Valeur | Interprétation |
|--------|---------------|
| 0 | Aucune victime |
| 1-3 | Accident léger |
| 4-9 | Accident grave |
| 10+ | Accident très grave (ex: accident de bus) |

### `score_gravite` (FAIT_ACCIDENT)

Normalisation min-max de `indice_gravite` sur une échelle de 1 à 5 :
```
score_gravite = 1 + 4 × (indice_gravite - min) / (max - min)
```

Utile pour comparer la gravité entre pays et années sur une échelle commune.

### `conditions` (DIM_METEO)

Condition météo du jour calculée depuis les précipitations, le vent et la température :

| Valeur | Règle |
|--------|-------|
| `pluie` | précipitations > 2mm |
| `vent_fort` | vent ≥ 40 m/s |
| `ensoleille` | temp_max ≥ 20°C et pas de pluie |
| `nuageux` | tous les autres cas |

> Seuil de 2mm choisi pour correspondre à la définition météorologique standard d'un jour de pluie à l'échelle nationale.

### `gravite` (DIM_USAGER)

Gravité de chaque usager impliqué dans l'accident :

| Valeur | Source FR (`grav`) | Source UK (`casualty_severity`) |
|--------|-------------------|--------------------------------|
| `Killed` | 2 | 1 |
| `Seriously injured` | 3 | 2 |
| `Slightly injured` | 4 | 3 |
| `Uninjured` | 1 | — |