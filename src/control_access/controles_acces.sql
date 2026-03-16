-- Création des rôles
CREATE ROLE francais;
CREATE ROLE anglais;
CREATE ROLE medecin;
CREATE ROLE policier;
CREATE ROLE admin_global;

-- admin_global : accès total à toutes les tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO admin_global;

-- Droits de lecture sur la table des faits
GRANT SELECT ON fait_accident TO francais, anglais;

-- Droits de lecture par métier
GRANT SELECT ON dim_usager   TO medecin;
GRANT SELECT ON dim_vehicule TO policier;

-- Activation du RLS sur fait_accident
ALTER TABLE fait_accident ENABLE ROW LEVEL SECURITY;

-- Politique FR : uniquement les accidents français
CREATE POLICY acces_pays_fr
ON fait_accident FOR SELECT TO francais
USING (id_pays = 1);

-- Politique UK : uniquement les accidents britanniques
CREATE POLICY acces_pays_uk
ON fait_accident FOR SELECT TO anglais
USING (id_pays = 2);

-- Activation du RLS sur dim_usager
ALTER TABLE dim_usager ENABLE ROW LEVEL SECURITY;

-- Politique médecin : uniquement blessés et tués
CREATE POLICY acces_medecin_select
ON dim_usager FOR SELECT TO medecin
USING (gravite IN ('Killed', 'Seriously injured'));

CREATE POLICY acces_medecin_update
ON dim_usager FOR UPDATE TO medecin
USING (gravite IN ('Killed', 'Seriously injured'));

-- Activation du RLS sur dim_vehicule
ALTER TABLE dim_vehicule ENABLE ROW LEVEL SECURITY;

-- Politique policier : accès à tous les véhicules
CREATE POLICY acces_policier
ON dim_vehicule FOR SELECT TO policier
USING (true);