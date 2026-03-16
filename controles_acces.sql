-- Création des roles 
CREATE ROLE francais;
CREATE ROLE anglais;

CREATE ROLE medecin;
CREATE ROLE policier;

CREATE ROLE admin_global;

-- Quelque soit le pays, droit de lecture de la table des faits
GRANT SELECT ON fait_accidents TO francais, anglais;



-- Selon le métier accès en lecture à différentes dimensions
GRANT SELECT ON DIM_USAGER TO medecin;
GRANT SELECT ON DIM_VEHICULE TO policier;



-- Chaque pays a uniquement accès aux données de son propre pays
CREATE POLICY acces_pays_fr
ON fait_accidents
FOR SELECT
TO francais
USING (id_pays = 1);

CREATE POLICY acces_pays_uk
ON fait_accidents
FOR SELECT
TO anglais
USING (id_pays = 2);



-- Activation du RLS sur dim_usager
ALTER TABLE dim_usager ENABLE ROW LEVEL SECURITY;

-- Restriction : uniquement gravite = 2 et 3 (blessés et tués)
-- Select et Update pour permettre de modifier la gravité
CREATE POLICY acces_medecin
ON dim_usager
FOR SELECT, UPDATE
TO medecin
USING (gravite IN (2, 3));



-- Activation du RLS sur dim_vehicule
ALTER TABLE dim_vehicule ENABLE ROW LEVEL SECURITY;

-- Politique : toutes les lignes visibles
CREATE POLICY acces_policier
ON dim_vehicule
FOR SELECT
TO policier
USING (true);