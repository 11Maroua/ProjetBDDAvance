# Politique de Sécurité — Row Level Security (RLS)

---

## Table des matières

1. [Contexte et objectifs](#1-contexte-et-objectifs)
2. [Architecture du schéma](#2-architecture-du-schéma)
3. [Rôles et utilisateurs](#3-rôles-et-utilisateurs)
4. [Table de mapping utilisateur ↔ pays](#4-table-de-mapping-utilisateur--pays)
5. [Droits d'accès par rôle](#5-droits-daccès-par-rôle)
6. [Mise en place du RLS](#6-mise-en-place-du-rls)
7. [Politiques de sécurité détaillées](#7-politiques-de-sécurité-détaillées)
8. [Fonction utilitaire](#8-fonction-utilitaire)
9. [Matrice de sécurité récapitulative](#9-matrice-de-sécurité-récapitulative)
10. [Limites et recommandations](#10-limites-et-recommandations)

---

## 1. Contexte et objectifs

Cet entrepôt de données centralise des informations sur les accidents de la route pour deux pays : la **France** (id_pays = 1) et le **Royaume-Uni** (id_pays = 2).

La politique de sécurité repose sur deux principes :

- **Isolation par pays** : chaque utilisateur ne peut accéder qu'aux données correspondant à son pays d'appartenance.
- **Restriction par rôle métier** : certains rôles (médecin, policier) ont un accès limité à certaines tables uniquement, en fonction de leur besoin métier.

Ces deux principes sont mis en place grâce au Row Level Security (RLS).
---

## 2. Architecture du schéma

| Table | Type | Colonne de filtrage RLS |
|---|---|---|
| `FAIT_ACCIDENT` | Table de faits | `id_pays` |
| `DIM_LOCALISATION` | Dimension | `id_pays` |
| `DIM_USAGER` | Dimension | `id_pays` |
| `DIM_VEHICULE` | Dimension | `id_pays` |
| `DIM_METEO` | Dimension | `id_pays` |
| `DIM_TEMPS` | Dimension | *(aucune — données partagées)* |
| `DIM_PAYS` | Dimension | *(aucune — données partagées)* |
| `UTILISATEUR_PAYS` | Table de sécurité | `utilisateur` |

> **Note :** `DIM_TEMPS` et `DIM_PAYS` ne contiennent pas de données sensibles liées à un pays spécifique. Le RLS y est activé avec une politique permissive (`USING (true)`) afin d'assurer la cohérence de la configuration tout en permettant l'accès à tous.

---

## 3. Rôles et utilisateurs

Cinq rôles sont définis dans le système :
- USER1 et USER2 sont des utilisateurs standards et ont accès aux données de leurs pays respectifs.
- MEDECIN a accès uniquement à DIM_USAGER et FAIT_ACCIDENT.
- POLICIER a accès uniquement à DIM_VEHICULE et FAIT_ACCIDENT.
- ADMIN_GLOBAL est administrateur et a un accès total, non soumis au RLS. 


## 4. Table de mapping utilisateur ↔ pays

La table `UTILISATEUR_PAYS` associe chaque nom d'utilisateur PostgreSQL à un identifiant de pays. Elle est consultée dynamiquement par la fonction RLS pour déterminer quelles lignes sont accessibles.

### Structure

```sql
CREATE TABLE UTILISATEUR_PAYS (
    utilisateur VARCHAR(20) PRIMARY KEY,
    id_pays     INTEGER
);
```

### Contenu initial

```sql
INSERT INTO UTILISATEUR_PAYS VALUES
    ('user1',    1),   -- Français
    ('user2',    2),   -- Anglais
    ('medecin',  1),   -- À adapter selon la nationalité
    ('policier', 1);   -- À adapter selon la nationalité
```


### Protection de la table

La table `UTILISATEUR_PAYS` est elle-même protégée par le RLS : chaque utilisateur ne peut lire que sa propre ligne, empêchant ainsi la découverte des mappings des autres comptes.

---

## 5. Droits d'accès par rôle

### Principe

Les `GRANT` définissent **quelles tables** un rôle peut voir. Le RLS détermine ensuite **quelles lignes** dans ces tables sont accessibles. Les deux mécanismes sont complémentaires et tous deux nécessaires.

### Tableau récapitulatif des accès aux tables

| Table | USER1 / USER2 | MEDECIN | POLICIER | ADMIN_GLOBAL |
|---|---|---|---|---|
| `FAIT_ACCIDENT` | SELECT | SELECT | SELECT | Tout |
| `DIM_LOCALISATION` | SELECT | SELECT | SELECT | Tout |
| `DIM_USAGER` | SELECT | SELECT | ❌ | Tout |
| `DIM_VEHICULE` | SELECT | ❌ | SELECT | Tout |
| `DIM_TEMPS` | SELECT | SELECT | SELECT | Tout |
| `DIM_PAYS` | SELECT | SELECT | SELECT | Tout |
| `DIM_METEO` | SELECT | ❌ | ❌ | Tout |
| `UTILISATEUR_PAYS` | Ligne propre | Ligne propre | Ligne propre | Tout |

---


## 6. Politiques de sécurité détaillées

### 6.1 Fonction utilitaire

Avant les politiques, une fonction réutilisable est définie pour récupérer le pays de l'utilisateur courant (voir [section 8](#8-fonction-utilitaire)).

### 6.2 Tables filtrées

Les tables fait_accident, dim_usager, dim_vehicule, dim_localisation et dim_meteo contiennent une colonne `id_pays` et sont filtrées en conséquence.

dim_temps et dim_pays sont accessibles à tous sans restriction de lignes.


---

## 7. Fonction utilitaire

Pour éviter de répéter la sous-requête dans chaque politique, une fonction SQL est définie pour attribuer à un utilisateur son pays d'origine.

| Attribut | Valeur | Explication |
|---|---|---|
| `RETURNS INTEGER` | Retourne l'`id_pays` | Entier correspondant au pays |
| `LANGUAGE SQL` | SQL natif | Pas de PL/pgSQL nécessaire ici |
| `SECURITY DEFINER` | Exécution avec les droits du créateur | Permet de lire `UTILISATEUR_PAYS` même si l'appelant n'y a pas accès directement |
| `STABLE` | Résultat stable dans une transaction | Permet à PostgreSQL d'optimiser les appels répétés |

> `SECURITY DEFINER` doit être utilisé avec précaution : la fonction s'exécute avec les droits de son créateur (typiquement un superuser). Elle ne doit pas exposer d'autres données que le `id_pays` de l'utilisateur courant.

---

## 8. Matrice de sécurité récapitulative

### Accès aux lignes par rôle et par pays

| Rôle | Pays | `FAIT_ACCIDENT` | `DIM_USAGER` | `DIM_VEHICULE` | `DIM_LOCALISATION` | `DIM_METEO` |
|---|---|---|---|---|---|---|
| USER1 | France (1) | Lignes FR | Lignes FR | Lignes FR | Lignes FR | Lignes FR |
| USER2 | R.-U. (2) | Lignes UK | Lignes UK | Lignes UK | Lignes UK | Lignes UK |
| MEDECIN | France (1) | Lignes FR | Lignes FR | *(pas de GRANT)* | Lignes FR | *(pas de GRANT)* |
| POLICIER | France (1) | Lignes FR | *(pas de GRANT)* | Lignes FR | Lignes FR | *(pas de GRANT)* |
| ADMIN_GLOBAL | — | Tout | Tout | Tout | Tout | Tout |

### Flux de contrôle d'accès

```
Requête utilisateur
        │
        ▼
  GRANT vérifié ?
  ┌─────┴─────┐
  NON        OUI
  │           │
Refus       RLS évalué
            │
     pays_utilisateur_courant()
            │
     Filtre sur id_pays
            │
      Lignes retournées
```