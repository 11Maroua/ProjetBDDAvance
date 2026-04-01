-- PARTIE 1 : Requête sélective (France, 2019)
-- Montre l'impact MAXIMAL des index sur une requête ciblée

-- SANS index
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;

EXPLAIN ANALYZE
SELECT COUNT(*) AS nb_accidents_mortels
FROM fait_accident f
JOIN dim_temps t ON f.date = t.date
JOIN dim_pays p ON f.id_pays = p.id_pays
WHERE f.nb_tues > 0
AND t.annee = 2019
AND f.id_pays = 1;

-- AVEC index
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;

EXPLAIN ANALYZE
SELECT COUNT(*) AS nb_accidents_mortels
FROM fait_accident f
JOIN dim_temps t ON f.date = t.date
JOIN dim_pays p ON f.id_pays = p.id_pays
WHERE f.nb_tues > 0
AND t.annee = 2019
AND f.id_pays = 1;

-- PARTIE 2 : Les limites de l'indexation
-- Requête peu sélective (toutes années, tous pays)
-- PostgreSQL préfère le Seq Scan parallélisé

-- ÉTAPE 1 : EXPLAIN ANALYZE SANS index
EXPLAIN ANALYZE
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

-- ÉTAPE 2 : Création des index
CREATE INDEX IF NOT EXISTS idx_dim_temps_annee           ON dim_temps(annee);
CREATE INDEX IF NOT EXISTS idx_dim_temps_mois            ON dim_temps(mois);
CREATE INDEX IF NOT EXISTS idx_dim_temps_saison          ON dim_temps(saison);
CREATE INDEX IF NOT EXISTS idx_dim_temps_weekend         ON dim_temps(est_weekend);
CREATE INDEX IF NOT EXISTS idx_fait_accident_id_pays     ON fait_accident(id_pays);
CREATE INDEX IF NOT EXISTS idx_fait_accident_id_lieu     ON fait_accident(id_lieu);
CREATE INDEX IF NOT EXISTS idx_fait_accident_id_usager   ON fait_accident(id_usager);
CREATE INDEX IF NOT EXISTS idx_fait_accident_id_vehicule ON fait_accident(id_vehicule);
CREATE INDEX IF NOT EXISTS idx_fait_accident_date        ON fait_accident(date);

-- ÉTAPE 3 : EXPLAIN ANALYZE AVEC index
EXPLAIN ANALYZE
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