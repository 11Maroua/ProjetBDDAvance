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
    t.est_jour_ferie,                          
    COUNT(DISTINCT f.id_accident)              AS nb_accidents,
    ROUND(AVG(f.indice_gravite)::numeric, 2)   AS gravite_moyenne
FROM FAIT_ACCIDENT f
JOIN DIM_TEMPS t ON t.date = f.date
JOIN DIM_METEO m ON m.id_pays = f.id_pays AND m.date = f.date
GROUP BY m.conditions, t.est_jour_ferie
ORDER BY gravite_moyenne DESC, nb_accidents DESC;