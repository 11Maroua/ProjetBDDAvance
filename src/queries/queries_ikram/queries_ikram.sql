-- Score de gravité : normalisation min-max de indice_gravite
-- sur une échelle 1 à 5 (indice_gravite conservé intact)
ALTER TABLE fait_accident
ADD COLUMN IF NOT EXISTS score_gravite NUMERIC(4,2);

UPDATE fait_accident
SET score_gravite = s.score_calc
FROM (
    SELECT
        id_accident,
        CASE
            WHEN max_ind = min_ind THEN 3.00
            ELSE ROUND(
                (1 + 4 * ((indice_gravite - min_ind) / NULLIF(max_ind - min_ind, 0)))::numeric,
                2
            )
        END AS score_calc
    FROM (
        SELECT
            id_accident,
            indice_gravite,
            MIN(indice_gravite) OVER () AS min_ind,
            MAX(indice_gravite) OVER () AS max_ind
        FROM fait_accident
    ) t
) s
WHERE fait_accident.id_accident = s.id_accident;

-- Requête 1 - Évolution du nombre d'accidents mortels (par année et par pays)
SELECT
    t.annee,
    p.nom_pays,
    COUNT(*) AS nb_accidents_mortels
FROM fait_accident f
JOIN dim_temps t ON f.date = t.date
JOIN dim_pays p ON f.id_pays = p.id_pays
WHERE f.nb_tues > 0
GROUP BY t.annee, p.nom_pays
ORDER BY t.annee, p.nom_pays;

-- Requête 2 - Gravité moyenne par année avec moyenne mobile sur 5 ans
SELECT
    t.annee,
    p.nom_pays,
    ROUND(AVG(f.score_gravite), 2) AS gravite_moyenne,
    ROUND(
        AVG(AVG(f.score_gravite)) OVER (
            PARTITION BY p.nom_pays
            ORDER BY t.annee
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS moyenne_mobile_5_ans
FROM fait_accident f
JOIN dim_temps t ON f.date = t.date
JOIN dim_pays p ON f.id_pays = p.id_pays
GROUP BY t.annee, p.nom_pays
ORDER BY t.annee, p.nom_pays;

-- Requête 3 - Température moyenne les jours d'accidents mortels vs jours sans accident mortel
WITH jours AS (
    SELECT
        f.date,
        f.id_pays,
        CASE
            WHEN SUM(f.nb_tues) > 0 THEN 'Jour avec accident mortel'
            ELSE 'Jour sans accident mortel'
        END AS type_jour
    FROM fait_accident f
    GROUP BY f.date, f.id_pays
)
SELECT
    j.type_jour,
    ROUND(AVG(m.temp_min)::numeric, 2) AS temp_min_moyenne,
    ROUND(AVG(m.temp_max)::numeric, 2) AS temp_max_moyenne
FROM jours j
JOIN dim_meteo m
    ON j.date = m.date
   AND j.id_pays = m.id_pays
GROUP BY j.type_jour;