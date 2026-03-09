# Projet BDD Avance - Accidents de la route et Meteo FR/UK

## Recuperer les donnees

Les donnees brutes ne sont pas sur GitHub (trop volumineuses).
Il faut les récuperer en les telechargeant en local :
```bash
pip install requests pandas
python3 src/scripts_recup_donnees/download_accidents_fr.py
python3 src/scripts_recup_donnees/download_accidents_uk.py
python3 src/scripts_recup_donnees/download_meteo_fr.py
```

Les donnees seront dans data/raw/.