# Analyse de l'impact des index

## Requête analysée

Évolution du nombre d'accidents mortels par année et par pays :

```sql
SELECT t.annee, p.nom_pays, COUNT(*) AS nb_accidents_mortels
FROM fait_accident f
JOIN dim_temps t ON f.date = t.date
JOIN dim_pays p ON f.id_pays = p.id_pays
WHERE f.nb_tues > 0
GROUP BY t.annee, p.nom_pays
ORDER BY t.annee, p.nom_pays;
```

## Index créés

### Sur `DIM_TEMPS`
| Index | Colonne | Justification |
|-------|---------|---------------|
| `idx_dim_temps_annee` | `annee` | Utilisé dans GROUP BY et ORDER BY |
| `idx_dim_temps_mois` | `mois` | Filtres temporels fréquents |
| `idx_dim_temps_saison` | `saison` | Analyses saisonnières |
| `idx_dim_temps_weekend` | `est_weekend` | Filtres jour ouvré / week-end |

### Sur `FAIT_ACCIDENT`
| Index | Colonne | Justification |
|-------|---------|---------------|
| `idx_fait_accident_date` | `date` | Clé de jointure principale avec DIM_TEMPS et DIM_METEO |
| `idx_fait_accident_id_pays` | `id_pays` | Clé étrangère vers DIM_PAYS |
| `idx_fait_accident_id_lieu` | `id_lieu` | Clé étrangère vers DIM_LOCALISATION |
| `idx_fait_accident_id_usager` | `id_usager` | Clé étrangère vers DIM_USAGER |
| `idx_fait_accident_id_vehicule` | `id_vehicule` | Clé étrangère vers DIM_VEHICULE |

## Comparaison des plans d'exécution

| Métrique | Sans index | Avec index | Variation |
|----------|-----------|------------|-----------|
| **Temps d'exécution** | 717.696 ms | 607.921 ms | **-15.3%** |
| **Temps de planification** | 6.515 ms | 7.205 ms | +10.6% |
| `shared hit` (cache) | 1 124 | 1 814 | +61% |
| `shared read` (disque) | 42 825 | 42 135 | -1.6% |

