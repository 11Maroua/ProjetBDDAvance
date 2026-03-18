-- Requête 1 — gravité moyenne par météo et pays
SELECT
    nom_pays,
    conditions,
    SUM(nb_accidents)                        AS nb_accidents,
    ROUND(AVG(gravite_moyenne)::numeric, 2)  AS gravite_moyenne,
    RANK() OVER (
        PARTITION BY nom_pays
        ORDER BY AVG(gravite_moyenne) DESC
    ) AS rang_conditions
FROM vm_accidents_complet
WHERE nom_pays IS NOT NULL AND conditions IS NOT NULL
GROUP BY nom_pays, conditions
ORDER BY nom_pays, rang_conditions;


-- Requête 2 — quel temps favorise les accidents graves
SELECT
    COALESCE(nom_pays, 'TOTAL')     AS nom_pays,
    COALESCE(conditions, 'TOUTES')  AS conditions,
    SUM(nb_accidents)               AS nb_accidents_graves,
    ROUND(AVG(gravite_moyenne)::numeric, 2) AS gravite_moyenne
FROM vm_accidents_complet
WHERE gravite_moyenne > 4
GROUP BY GROUPING SETS (
    (nom_pays, conditions),
    (nom_pays),
    ()
)
ORDER BY nom_pays, gravite_moyenne DESC;


-- Requête 3 — zones les plus accidentogènes avec moyenne mobile
SELECT
    nom_pays,
    zone,
    conditions,
    SUM(nb_accidents)                        AS nb_accidents_graves,
    ROUND(AVG(gravite_moyenne)::numeric, 2)  AS gravite_moyenne,
    ROUND(AVG(AVG(gravite_moyenne)) OVER (
        PARTITION BY nom_pays, zone
        ORDER BY conditions
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    )::numeric, 2) AS gravite_lissee
FROM vm_accidents_complet
WHERE gravite_moyenne > 4 AND zone IS NOT NULL
GROUP BY nom_pays, zone, conditions
HAVING SUM(nb_accidents) >= 50
ORDER BY gravite_moyenne DESC
LIMIT 20;