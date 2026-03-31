-- Comparaison FR vs UK : nb accidents par sexe et par tranche de gravité 

SELECT p.nom_pays, u.sexe, f.indice_gravite, COUNT(DISTINCT f.id_accident) AS nb_accidents
FROM FAIT_ACCIDENT f 
JOIN DIM_PAYS p ON p.id_pays = f.id_pays
JOIN DIM_USAGER u ON u.id_usager = f.id_usager 
GROUP BY ROLLUP(p.nom_pays, u.sexe, f.indice_gravite)
ORDER BY p.nom_pays NULLS LAST, f.indice_gravite NULLS LAST 
LIMIT 10;

-- Les accidents sont-ils plus graves pendant les jours feriés selon la météo

SELECT
    m.conditions,                           
    COUNT(DISTINCT f.id_accident)          AS nb_accidents,
    ROUND(AVG(f.indice_gravite)::numeric,    2)     AS gravite_moyenne,
    ROUND(AVG(f.nb_tues)::numeric,           2)     AS tues_par_accident,
    ROUND(AVG(f.nb_blesses_graves)::numeric, 2)     AS blesses_graves_par_accident,
    SUM(f.nb_victimes_total)               AS nb_victimes_total
FROM FAIT_ACCIDENT  f
    JOIN DIM_TEMPS t ON t.date = f.date
    JOIN DIM_METEO m ON m.id_pays = f.id_pays AND m.date = f.date
WHERE t.est_jour_ferie IS true
GROUP BY (m.conditions)

ORDER BY gravite_moyenne DESC;