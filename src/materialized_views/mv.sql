-- vue matérialisée principale de notre entrepot, lien entre les différentes dimensions et le fait, avec des agrégats pré-calculés pour accélérer les requêtes analytiques
CREATE MATERIALIZED VIEW vm_accidents_complet AS
SELECT
    p.nom_pays,
    t.annee,
    t.saison,
    t.est_weekend,
    t.est_jour_ferie,
    m.conditions,
    COALESCE(l.departement, l.district) AS zone,
    l.type_route,
    u.sexe,
    u.cat_usager,

    -- Agrégats
    COUNT(*)                                 AS nb_accidents,
    SUM(f.nb_tues)                           AS total_tues,
    SUM(f.nb_blesses_graves)                 AS total_blesses_graves,
    SUM(f.nb_blesses_legers)                 AS total_blesses_legers,
    SUM(f.nb_victimes_total)                 AS total_victimes,
    ROUND(AVG(f.indice_gravite)::numeric, 2) AS gravite_moyenne,
    MIN(f.indice_gravite)                    AS gravite_min,
    MAX(f.indice_gravite)                    AS gravite_max,
    ROUND(AVG(m.temp_min)::numeric, 2)       AS temp_min_moyenne,
    ROUND(AVG(m.temp_max)::numeric, 2)       AS temp_max_moyenne

FROM fait_accident f
JOIN dim_temps        t ON f.date        = t.date
JOIN dim_pays         p ON f.id_pays     = p.id_pays
JOIN dim_localisation l ON f.id_lieu     = l.id_lieu
JOIN dim_usager       u ON f.id_usager   = u.id_usager
JOIN dim_vehicule     v ON f.id_vehicule = v.id_vehicule
JOIN dim_meteo        m ON f.id_pays     = m.id_pays AND f.date = m.date

GROUP BY ROLLUP (
    p.nom_pays,
    t.annee,
    t.saison,
    t.est_weekend,
    t.est_jour_ferie,
    m.conditions,
    l.departement, l.district, l.type_route,
    u.sexe, u.cat_usager
);

-- Index sur la vue
CREATE INDEX idx_vm_pays       ON vm_accidents_complet(nom_pays);
CREATE INDEX idx_vm_annee      ON vm_accidents_complet(annee);
CREATE INDEX idx_vm_conditions ON vm_accidents_complet(conditions);
CREATE INDEX idx_vm_saison     ON vm_accidents_complet(saison);