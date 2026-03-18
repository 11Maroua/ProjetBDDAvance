-- Requêtes Maroua —-

-- 1. Gravité moyenne des accidents selon les conditions météo et le pays
SELECT 
    p.nom_pays,
    m.conditions,
    ROUND(AVG(f.indice_gravite)::numeric, 2) AS gravite_moyenne,
    COUNT(*) AS nb_accidents
FROM fait_accident f
JOIN dim_pays  p ON f.id_pays = p.id_pays
JOIN dim_meteo m ON f.id_pays = m.id_pays AND f.date::date = m.date
GROUP BY p.nom_pays, m.conditions
ORDER BY p.nom_pays, gravite_moyenne DESC;

-- 2. Quel temps favorise le plus les accidents graves ?
SELECT 
    m.conditions,
    COUNT(*) AS nb_accidents_graves,
    ROUND(AVG(f.indice_gravite)::numeric, 2) AS gravite_moyenne
FROM fait_accident f
JOIN dim_meteo m ON f.id_pays = m.id_pays AND f.date = m.date
WHERE f.indice_gravite > 4
GROUP BY m.conditions
ORDER BY gravite_moyenne DESC;

-- 3. Où y a-t-il le plus d'accidents graves ? Est-ce lié aux conditions météo ?
SELECT 
    p.nom_pays,
    COALESCE(l.departement, l.district) AS zone,
    m.conditions,
    COUNT(*) AS nb_accidents_graves,
    ROUND(AVG(f.indice_gravite)::numeric, 2) AS gravite_moyenne
FROM fait_accident f
JOIN dim_localisation l ON f.id_lieu = l.id_lieu
JOIN dim_meteo m ON f.id_pays = m.id_pays AND f.date = m.date
JOIN dim_pays p ON f.id_pays = p.id_pays
WHERE f.indice_gravite > 4
GROUP BY p.nom_pays, l.departement, l.district, m.conditions
HAVING COUNT(*) >= 50
ORDER BY gravite_moyenne DESC
LIMIT 20;