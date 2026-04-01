# Analyse de l'impact des index — IKRAM

## Index créés

### Sur `DIM_TEMPS`
| Index | Colonne | Justification |
|-------|---------|---------------|
| `idx_dim_temps_annee` | `annee` | Utilisé dans GROUP BY, ORDER BY et filtres temporels |
| `idx_dim_temps_mois` | `mois` | Analyses mensuelles |
| `idx_dim_temps_saison` | `saison` | Analyses saisonnières |
| `idx_dim_temps_weekend` | `est_weekend` | Filtres jour ouvré / week-end |

### Sur `FAIT_ACCIDENT`
| Index | Colonne | Justification |
|-------|---------|---------------|
| `idx_fait_accident_date` | `date` | Clé de jointure principale avec DIM_TEMPS et DIM_METEO |
| `idx_fait_accident_id_pays` | `id_pays` | Clé étrangère vers DIM_PAYS + filtre fréquent |
| `idx_fait_accident_id_lieu` | `id_lieu` | Clé étrangère vers DIM_LOCALISATION |
| `idx_fait_accident_id_usager` | `id_usager` | Clé étrangère vers DIM_USAGER |
| `idx_fait_accident_id_vehicule` | `id_vehicule` | Clé étrangère vers DIM_VEHICULE |

---

## PARTIE 1 — Requête sélective : impact maximal des index

**Requête :** Nombre d'accidents mortels en France en 2019

```sql
SELECT COUNT(*) AS nb_accidents_mortels
FROM fait_accident f
JOIN dim_temps t ON f.date = t.date
JOIN dim_pays p ON f.id_pays = p.id_pays
WHERE f.nb_tues > 0
AND t.annee = 2019
AND f.id_pays = 1;
```

| Métrique | Sans index | Avec index | Variation |
|----------|-----------|------------|-----------|
| **Temps d'exécution** | 733.931 ms | 31.133 ms | **-96%** |
| **Temps de planification** | 4.417 ms | 0.387 ms | **-91%** |
| `shared hit` (cache) | 58 | 7 476 | **+12 786%** |
| `shared read` (disque) | 43 861 | 0 | **-100%** |

### Pourquoi un gain aussi important ?

Avec les filtres `t.annee = 2019` et `f.id_pays = 1`, la requête est très **sélective** :
- `idx_dim_temps_annee` → PostgreSQL trouve directement les 365 jours de 2019 sans scanner toute la table
- `idx_fait_accident_id_pays` → PostgreSQL trouve directement les accidents français (Bitmap Index Scan)
- `dim_pays_pkey` → Index Only Scan, pas de lecture disque du tout

Le plan d'exécution passe de **Seq Scan** (lecture complète) à **Bitmap Index Scan + Index Scan** → lecture ciblée.

---

## PARTIE 2 — Requête peu sélective : limites de l'indexation

**Requête :** Évolution des accidents mortels par année et par pays (toutes années, tous pays)

| Métrique | Sans index | Avec index | Variation |
|----------|-----------|------------|-----------|
| **Temps d'exécution** | 748.568 ms | 681.525 ms | **-9%** |
| **Temps de planification** | 7.429 ms | 1.666 ms | **-78%** |
| `shared hit` (cache) | 88 | 402 | **+357%** |
| `shared read` (disque) | 43 861 | 43 547 | **-0.7%** |

### Pourquoi le gain est modéré ici ?

La requête retourne ~51 000 lignes sur 1,9 millions — soit 2,6% de la table.
PostgreSQL choisit un **Parallel Seq Scan** (scan séquentiel avec 2 workers parallèles) car :
1. Le volume de résultats est trop grand pour qu'un index scan soit rentable
2. Le scan parallélisé lit la table entière très rapidement en utilisant plusieurs processeurs

Les index restent utiles : le temps de planification baisse de 78% et le cache hit augmente de 357%.

---

## Conclusion

L'indexation est **maximalement efficace** sur des requêtes **sélectives** (filtres précis sur peu de valeurs) : **-96%** de temps d'exécution.

Sur des requêtes **analytiques larges** (toutes années, tous pays), l'impact est plus modéré (**-9%**) car PostgreSQL préfère le scan séquentiel parallélisé. Les index restent néanmoins indispensables pour les clés étrangères et les requêtes ponctuelles.