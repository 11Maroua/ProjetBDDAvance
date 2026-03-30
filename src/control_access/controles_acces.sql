-- Création des rôles
CREATE ROLE USER1;
CREATE ROLE USER2;

CREATE ROLE MEDECIN;
CREATE ROLE POLICIER;
CREATE ROLE ADMIN_USER;
CREATE ROLE ADMIN_GLOBAL LOGIN BYPASSRLS;

-- Création d'une table indiquant la nationalité de chaque user
CREATE TABLE UTILISATEUR_PAYS(
utilisateur VARCHAR(20) PRIMARY KEY,
pays INTEGER
);

-- Remplissage de la table UTILISATEUR_PAYS
INSERT INTO UTILISATEUR_PAYS VALUES
('USER1',1),    -- français
('USER2',2),    -- anglais
('MEDECIN',1),
('POLICIER',2);

-- Droits pour l'administrateur 
GRANT SELECT ON UTILISATEUR_PAYS TO ADMIN_GLOBAL;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ADMIN_GLOBAL;

-- Droits pour les autres rôles 
GRANT SELECT ON fait_accident TO MEDCIN;
GRANT SELECT ON fait_accident TO POLICIER;
GRANT SELECT ON dim_usager   TO MEDECIN;
GRANT SELECT ON dim_vehicule TO POLICIER;

-- Droits pour les user
GRANT SELECT ON fait_accident TO ADMIN_USER;


-- Activation du RLS sur la table des faits
ALTER TABLE FAIT_ACCIDENT ENABLE ROW LEVEL SECURITY;

-- Activation du RLS sur les dimensions
ALTER TABLE dim_usager ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_vehicule ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_localisation ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_temps ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_pays ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_meteo ENABLE ROW LEVEL SECURITY;

-- Sous-requête réutilisable (pour éviter la répétition)
CREATE OR REPLACE FUNCTION pays_utilisateur_courant()
RETURNS INTEGER
LANGUAGE SQL
SECURITY DEFINER   -- s'exécute avec les droits du créateur, pas de l'appelant
STABLE
AS $$
    SELECT id_pays
    FROM UTILISATEUR_PAYS
    WHERE utilisateur = current_user;
$$;

-- Politique de sécurité sur la table des faits
DROP POLICY acces_pays ON fait_accident;
CREATE POLICY acces_pays
ON fait_accident
FOR SELECT
USING (
    id_pays = pays_utilisateur_courant()
);

-- Politique sur DIM_USAGER
CREATE POLICY acces_pays
ON DIM_USAGER
FOR SELECT
USING (
    id_pays = pays_utilisateur_courant()
);

-- Politique sur DIM_VEHICULE
CREATE POLICY acces_pays
ON DIM_VEHICULE
FOR SELECT
USING (
    id_pays = pays_utilisateur_courant()
);

-- Protection de la table UTILISATEUR_PAYS
ALTER TABLE UTILISATEUR_PAYS ENABLE ROW LEVEL SECURITY;

CREATE POLICY acces_propre_ligne
ON UTILISATEUR_PAYS
FOR SELECT
USING (
    utilisateur = current_user
);

-- DIM_TEMPS et DIM_PAYS : pas de filtre pays, accès libre
DROP POLICY IF EXISTS tout_voir ON dim_temps;
CREATE POLICY tout_voir ON dim_temps FOR SELECT USING (true);

DROP POLICY IF EXISTS tout_voir ON dim_pays;
CREATE POLICY tout_voir ON dim_pays FOR SELECT USING (true);


-- L'administrateur voit tout 
GRANT SELECT ON UTILISATEUR_PAYS TO ADMIN_GLOBAL;