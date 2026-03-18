-- Comparaison FR vs UK : nb accidents par sexe et par tranche de gravité 

SELECT p.nom_pays, COUNT(DISTINCT f.id_accident) AS nb_accidents
FROM FAIT_ACCIDENT f 
    JOIN DIM_PAYS p ON p.id_pays = f.id_pays
    JOIN DIM_USAGER u ON u.id_pays = f.id_pays

GROUP BY p.code_pays, u.sexe, f.indice_gravite
ORDER BY p.id_pays, f.indice_gravite

-- tranche de gravité qu'est ce qu'on voulait dire ??
-- ajout roll up ?

    -- Les accidents sont-ils plus graves un jour férié selon la météo 
