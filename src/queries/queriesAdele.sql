-- Comparaison FR vs UK : nb accidents par sexe et par tranche de gravité 

SELECT p.nom_pays, u.sexe, f.indice_gravite, COUNT(DISTINCT f.id_accident) AS nb_accidents
FROM FAIT_ACCIDENT f 
    JOIN DIM_PAYS p ON p.id_pays = f.id_pays
    JOIN DIM_USAGER u ON u.id_pays = f.id_pays

GROUP BY ROLLUP(p.nom_pays, u.sexe, f.indice_gravite)
ORDER BY p.nom_pays_pays, f.indice_gravite -- vrmt nécessaire ?

-- tranche de gravité qu'est ce qu'on voulait dire ??



-- Les accidents sont-ils plus graves pendant les jours feriés selon la météo
"""
SELECT f.id_accident, 
FROM fait_accident f 
    JOIN dim_meteo m ON f.id_pays = m.id_pays AND f.date::date = m.date
WHERE 
GROUP BY m.precipitations, m.conditions

"""
SELECT
    MAX(d.est_ferie_fr, d.est_ferie_uk)    AS est_ferie         --si est_ferie = 1 dans un des deux pays alors le jour est considéré ferié
    --d.est_weekend,
    m.conditions,                           
    COUNT(DISTINCT f.id_accident)          AS nb_accidents,
    ROUND(AVG(f.indice_gravite),    2)     AS gravite_moyenne,
    ROUND(AVG(f.nb_tues),           2)     AS tues_par_accident,
    ROUND(AVG(f.nb_blesses_graves), 2)     AS blesses_graves_par_accident,
    SUM(f.nb_victimes_total)               AS nb_victimes_total,
    GROUPING(d.est_ferie) AS est_ferie                          -- A CHECK
FROM fait_accident  f
    JOIN DIM_TEMPS d ON d.date = f.date
    JOIN DIM_METEO m ON m.id_pays = f.id_pays AND m.date = f.date
-- WHERE      d.est_weekend IS NOT NULL     -- filtre si la colonne est nullable : NECESSAIRE?
GROUP BY CUBE(d.est_ferie, m.conditions, gravite_moyenne)
-- GROUP BY ROLLUP(d.est_weekend, m.conditions, tranche_precipitations)

ORDER BY
    d.est_ferie DESC,                     -- jours fériés en premier
    gravite_moyenne DESC;

"""
GROUP BY GROUPING SETS (
    (d.est_ferie m.conditions),   -- croisement principal
    (d.est_ferie),                 -- total par type de jour
    (m.conditions),                  -- total par météo
    ()                               -- grand total
)
"""

-- ajoutez GROUPING(d.est_weekend) dans le SELECT pour distinguer les lignes de totaux (1) des lignes 
-- de détail (0), sinon les NULL introduits par ROLLUP/CUBE sont ambigus avec des NULL de données 
-- réelles 