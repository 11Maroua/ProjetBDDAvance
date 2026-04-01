-- Comparaison FR vs UK : gravité des accidents par sexe
SELECT
    p.nom_pays,
    u.sexe,
    COUNT(DISTINCT f.id_accident)                AS nb_accidents,
    ROUND(AVG(f.indice_gravite)::numeric, 2)     AS gravite_moyenne,

    -- Rang par pays : quel sexe a la gravité la plus élevée ?
    RANK() OVER (
        PARTITION BY p.nom_pays
        ORDER BY AVG(f.indice_gravite) DESC
    ) AS rang_gravite,

    -- Part relative dans le total des accidents du pays
    ROUND(
        COUNT(DISTINCT f.id_accident)::numeric /
        SUM(COUNT(DISTINCT f.id_accident)) OVER (PARTITION BY p.nom_pays) * 100
    , 1) AS pct_accidents_pays,

    -- Écart à la moyenne tous sexes confondus
    ROUND((
        AVG(f.indice_gravite) -
        AVG(AVG(f.indice_gravite)) OVER (PARTITION BY p.nom_pays)
    )::numeric, 2) AS ecart_moyenne_pays

FROM FAIT_ACCIDENT f
JOIN DIM_PAYS   p ON p.id_pays   = f.id_pays
JOIN DIM_USAGER u ON u.id_usager = f.id_usager
WHERE u.sexe IN ('Male', 'Female')
GROUP BY p.nom_pays, u.sexe
ORDER BY p.nom_pays, rang_gravite;

-- Les accidents sont-ils plus graves pendant les jours feriés selon la météo
SELECT
    m.conditions,
    t.est_jour_ferie,                          
    COUNT(DISTINCT f.id_accident)              AS nb_accidents,
    ROUND(AVG(f.indice_gravite)::numeric, 2)   AS gravite_moyenne
FROM FAIT_ACCIDENT f
JOIN DIM_TEMPS t ON t.date = f.date
JOIN DIM_METEO m ON m.id_pays = f.id_pays AND m.date = f.date
GROUP BY m.conditions, t.est_jour_ferie
ORDER BY gravite_moyenne DESC, nb_accidents DESC;