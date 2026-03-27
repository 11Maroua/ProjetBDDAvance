import holidays
import pandas as pd

def build_temps():
    # DIM_TEMPS


    ANNEES = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021]

    jours_feries_fr = holidays.France(years=ANNEES)
    jours_feries_uk = holidays.UnitedKingdom(years=ANNEES)

    JOURS_FR = {
        0: "Lundi", 1: "Mardi", 2: "Mercredi", 3: "Jeudi",
        4: "Vendredi", 5: "Samedi", 6: "Dimanche"
    }
    MOIS_FR = {
        1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril",
        5: "Mai", 6: "Juin", 7: "Juillet", 8: "Août",
        9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre"
    }

    rows = []
    id_temps = 1

    for annee in ANNEES:
        for mois in range(1, 13):
            for jour in range(1, 32):
                try:
                    date = pd.Timestamp(year=annee, month=mois, day=jour)
                except ValueError:
                    continue

                rows.append({
                    "id_temps":     id_temps,
                    "date":         date.strftime("%Y-%m-%d"),
                    "jour":         date.day,
                    "mois":         date.month,
                    "annee":        date.year,
                    "trimestre":    date.quarter,
                    "jour_semaine": date.dayofweek + 1,
                    "nom_jour":     JOURS_FR[date.dayofweek],
                    "nom_mois":     MOIS_FR[date.month],
                    "est_weekend":  int(date.dayofweek >= 5),
                    "est_ferie_fr": int(date in jours_feries_fr),
                    "est_ferie_uk": int(date in jours_feries_uk),
                    "saison":       (
                        "Hiver"     if mois in [12, 1, 2] else
                        "Printemps" if mois in [3, 4, 5]  else
                        "Ete"       if mois in [6, 7, 8]  else
                        "Automne"
                    ),
                })
                id_temps += 1

    dim_temps = pd.DataFrame(rows)
    dim_temps.to_csv("data/processed/DIM_TEMPS.csv", index=False)
    print(f"DIM_TEMPS générée : {len(dim_temps)} lignes")
    print(dim_temps.head(3))