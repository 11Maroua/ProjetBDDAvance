# Analyse des performances — Vues matérialisées vs requêtes directes


## Résultats obtenus: sans vue matérialisée VS avec vue matérialisée :

| Requête | Sans vue | Avec vue | Gain |
|---------|----------|----------|------|
| Gravité moyenne par météo et pays | 660 ms | 0.4 ms | ×1650 |
| Accidents graves par conditions météo | 57 ms | 0.017 ms | ×3353 |
| Zones les plus accidentogènes | 534 ms | 0.020 ms | ×26700 |

---

## Analyse requête par requête

### Requête 1 — Gravité moyenne par météo et pays

**Sans vue — 660 ms**
PostgreSQL effectue :
- Un `Parallel Seq Scan` sur `fait_accident` — lit les 1.9M lignes entières
- Deux `Hash Join` pour joindre `dim_pays` et `dim_meteo`
- Un `HashAggregate` pour calculer les moyennes

**Avec vue — 0.4 ms**
La vue est déjà agrégée et stockée. PostgreSQL fait un simple `Seq Scan` sur 1 ligne puis applique le `RANK()` OVER dessus. Aucune jointure, aucun recalcul.

---

### Requête 2 — Accidents graves par conditions météo

**Sans vue — 57 ms**
PostgreSQL scanne `fait_accident` en parallèle avec un filtre `indice_gravite > 4` qui élimine 97% des lignes qui sont lues pour rien... Puis Hash Join avec `dim_meteo`.

**Avec vue — 0.017 ms**
Simple `Seq Scan` sur la vue avec filtre `gravite_moyenne > 4`. Le `GROUPING SETS` est résolu en une passe (`MixedAggregate`) sur quelques lignes déjà agrégées.

---

### Requête 3 — Zones les plus accidentogènes

**Sans vue — 534 ms**
C'est la requête la plus coûteuse sans vue — 4 jointures dont un `Parallel Hash Join` entre `fait_accident` et `dim_localisation` (642 856 lignes chacune). Le tri final consomme jusqu'à 1743 MB de mémoire par worker.

**Avec vue — 0.020 ms**
La vue contient déjà la colonne `zone` pré-calculée avec `COALESCE(departement, district)`. PostgreSQL n'a plus qu'à filtrer et trier quelques lignes. La fonction de fenêtre `AVG() OVER()` s'applique sur un volume beaucoup plus négligeable que sans la vue.


---

## Limites des vues matérialisées

- Cela dit, la vue doit être **rafraîchie manuellement** si les données changent :
```sql
REFRESH MATERIALIZED VIEW vm_accidents_complet;
```

