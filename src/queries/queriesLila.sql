
-- Le nombre d'accidents est-il plus élevé les jours de pluie/neige ou temps sec 
SELECT conditions,SUM(f.nb_victimes_total) as nb_tue_meteo
FROM FAIT_ACCIDENT f JOIN DIM_METEO m 
ON (f.id_pays = m.id_pays and f.date = m.date)
GROUP BY conditions;

/*  conditions | nb_tue_meteo 
------------+--------------
 ensoleille |       172441
 nuageux    |       748207
 pluie      |      1645907 */


-- Explain plan without indexes 

/*                                                                           QUERY PLAN                                                                           
----------------------------------------------------------------------------------------------------------------------------------------------------------------
Finalize GroupAggregate  (cost=37384.65..37385.41 rows=3 width=15) (actual time=278.397..287.039 rows=3 loops=1)
   Group Key: m.conditions
   ->  Gather Merge  (cost=37384.65..37385.35 rows=6 width=15) (actual time=278.389..287.029 rows=9 loops=1)
         Workers Planned: 2
         Workers Launched: 2
         ->  Sort  (cost=36384.63..36384.64 rows=3 width=15) (actual time=259.367..259.370 rows=3 loops=3)
               Sort Key: m.conditions
               Sort Method: quicksort  Memory: 25kB
               Worker 0:  Sort Method: quicksort  Memory: 25kB
               Worker 1:  Sort Method: quicksort  Memory: 25kB
               ->  Partial HashAggregate  (cost=36384.58..36384.61 rows=3 width=15) (actual time=259.325..259.327 rows=3 loops=3)
                     Group Key: m.conditions
                     Batches: 1  Memory Usage: 24kB
                     Worker 0:  Batches: 1  Memory Usage: 24kB
                     Worker 1:  Batches: 1  Memory Usage: 24kB
                     ->  Hash Join  (cost=233.25..32372.19 rows=802478 width=11) (actual time=1.905..178.013 rows=642856 loops=3)
                           Hash Cond: ((f.id_pays = m.id_pays) AND (f.date = m.date))
                           ->  Parallel Seq Scan on fait_accident f  (cost=0.00..27918.70 rows=803570 width=12) (actual time=0.030..46.476 rows=642856 loops=3)
                           ->  Hash  (cost=134.70..134.70 rows=6570 width=15) (actual time=1.789..1.790 rows=6570 loops=3)
                                 Buckets: 8192  Batches: 1  Memory Usage: 375kB
                                 ->  Seq Scan on dim_meteo m  (cost=0.00..134.70 rows=6570 width=15) (actual time=0.013..0.762 rows=6570 loops=3)
 Planning Time: 0.171 ms
 Execution Time: 287.088 ms
*/


-- Top 10 des jours de l'année avec le plus d'accidents avec météo associée 
SELECT f.date,conditions,COUNT(*) AS nb_accident
FROM FAIT_ACCIDENT f JOIN DIM_METEO m 
ON (f.id_pays = m.id_pays and f.date = m.date)
GROUP BY f.date,conditions
ORDER BY COUNT(*) DESC
LIMIT 10;

/*  date    | conditions | nb_accident 
------------+------------+-------------
 2005-10-21 | pluie      |        1129
 2005-09-30 | pluie      |        1113
 2005-11-18 | nuageux    |        1109
 2005-06-24 | pluie      |        1085
 2005-11-04 | pluie      |        1060
 2005-12-02 | pluie      |        1060
 2005-12-07 | pluie      |        1045
 2007-09-28 | pluie      |        1039
 2005-12-16 | pluie      |        1031 
 2005-11-25 | pluie      |        1017*/

/*                                                                              QUERY PLAN                                                                              
---------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=41921.20..41921.22 rows=10 width=19) (actual time=269.283..271.971 rows=10 loops=1)
   ->  Sort  (cost=41921.20..41945.85 rows=9861 width=19) (actual time=269.282..271.970 rows=10 loops=1)
         Sort Key: (count(*)) DESC
         Sort Method: top-N heapsort  Memory: 26kB
         ->  Finalize HashAggregate  (cost=41609.50..41708.11 rows=9861 width=19) (actual time=268.407..271.557 rows=4994 loops=1)
               Group Key: f.date, m.conditions
               Batches: 1  Memory Usage: 913kB
               ->  Gather  (cost=39390.77..41461.58 rows=19722 width=19) (actual time=263.939..268.247 rows=14892 loops=1)
                     Workers Planned: 2
                     Workers Launched: 2
                     ->  Partial HashAggregate  (cost=38390.77..38489.38 rows=9861 width=19) (actual time=249.024..249.724 rows=4964 loops=3)
                           Group Key: f.date, m.conditions
                           Batches: 1  Memory Usage: 913kB
                           Worker 0:  Batches: 1  Memory Usage: 913kB
                           Worker 1:  Batches: 1  Memory Usage: 913kB
                           ->  Hash Join  (cost=233.25..32372.19 rows=802478 width=11) (actual time=1.589..156.715 rows=642856 loops=3)
                                 Hash Cond: ((f.id_pays = m.id_pays) AND (f.date = m.date))
                                 ->  Parallel Seq Scan on fait_accident f  (cost=0.00..27918.70 rows=803570 width=8) (actual time=0.021..44.266 rows=642856 loops=3)
                                 ->  Hash  (cost=134.70..134.70 rows=6570 width=15) (actual time=1.502..1.503 rows=6570 loops=3)
                                       Buckets: 8192  Batches: 1  Memory Usage: 375kB
                                       ->  Seq Scan on dim_meteo m  (cost=0.00..134.70 rows=6570 width=15) (actual time=0.010..0.597 rows=6570 loops=3)
 Planning Time: 0.195 ms
 Execution Time: 272.094 ms
(23 rows)
*/

-- Où y a-t-il le plus d'accidents graves, est-ce lié aux conditions météo
SELECT id_pays,SUM(indice_gravite)
FROM FAIT_ACCIDENT
GROUP BY id_pays;

/* id_pays |   sum   
---------+---------
       1 | 1141276
       2 | 2026329 */

/*                                                                        QUERY PLAN                                                                       
--------------------------------------------------------------------------------------------------------------------------------------------------------
 Finalize GroupAggregate  (cost=32936.60..32937.11 rows=2 width=12) (actual time=149.603..156.280 rows=2 loops=1)
   Group Key: id_pays
   ->  Gather Merge  (cost=32936.60..32937.07 rows=4 width=12) (actual time=149.598..156.274 rows=6 loops=1)
         Workers Planned: 2
         Workers Launched: 2
         ->  Sort  (cost=31936.58..31936.58 rows=2 width=12) (actual time=130.712..130.713 rows=2 loops=3)
               Sort Key: id_pays
               Sort Method: quicksort  Memory: 25kB
               Worker 0:  Sort Method: quicksort  Memory: 25kB
               Worker 1:  Sort Method: quicksort  Memory: 25kB
               ->  Partial HashAggregate  (cost=31936.55..31936.57 rows=2 width=12) (actual time=130.671..130.673 rows=2 loops=3)
                     Group Key: id_pays
                     Batches: 1  Memory Usage: 24kB
                     Worker 0:  Batches: 1  Memory Usage: 24kB
                     Worker 1:  Batches: 1  Memory Usage: 24kB
                     ->  Parallel Seq Scan on fait_accident  (cost=0.00..27918.70 rows=803570 width=12) (actual time=0.026..43.824 rows=642856 loops=3)
 Planning Time: 0.064 ms
 Execution Time: 156.313 ms
(18 rows)
*/

SELECT id_pays,id_lieu,SUM(indice_gravite) as gravite
FROM FAIT_ACCIDENT NATURAL JOIN DIM_LOCALISATION 
GROUP BY id_pays,id_lieu
ORDER BY gravite DESC
LIMIT 10;

/*  id_pays | id_lieu | gravite 
---------+---------+---------
       1 |  389118 |     141
       1 |  127282 |     126
       2 |  903822 |     116
       2 | 1161879 |      89
       2 | 1346466 |      79
       1 |  138754 |      78
       2 | 1184790 |      76
       1 |     929 |      67
       2 | 1748488 |      66
       1 |  324077 |      64 */

/*                                                                         QUERY PLAN                                                                        
----------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=270283.37..270283.39 rows=10 width=16) (actual time=2681.678..2681.683 rows=10 loops=1)
   ->  Sort  (cost=270283.37..273034.46 rows=1100435 width=16) (actual time=2676.773..2676.777 rows=10 loops=1)
         Sort Key: (sum(fait_accident.indice_gravite)) DESC
         Sort Method: top-N heapsort  Memory: 25kB
         ->  HashAggregate  (cost=224752.58..246503.36 rows=1100435 width=16) (actual time=1897.782..2545.842 rows=1928568 loops=1)
               Group Key: fait_accident.id_pays, fait_accident.id_lieu
               Planned Partitions: 32  Batches: 161  Memory Usage: 4153kB  Disk Usage: 95336kB
               ->  Hash Join  (cost=75843.20..151504.87 rows=1100435 width=16) (actual time=615.797..1479.021 rows=1928568 loops=1)
                     Hash Cond: ((fait_accident.id_pays = dim_localisation.id_pays) AND (fait_accident.id_lieu = dim_localisation.id_lieu))
                     ->  Seq Scan on fait_accident  (cost=0.00..39168.68 rows=1928568 width=16) (actual time=0.011..183.069 rows=1928568 loops=1)
                     ->  Hash  (cost=39380.68..39380.68 rows=1928568 width=8) (actual time=611.964..611.965 rows=1928568 loops=1)
                           Buckets: 131072  Batches: 32  Memory Usage: 3378kB
                           ->  Seq Scan on dim_localisation  (cost=0.00..39380.68 rows=1928568 width=8) (actual time=0.421..332.883 rows=1928568 loops=1)
 Planning Time: 0.184 ms
 JIT:
   Functions: 19
   Options: Inlining false, Optimization false, Expressions true, Deforming true
   Timing: Generation 0.770 ms, Inlining 0.000 ms, Optimization 0.461 ms, Emission 7.356 ms, Total 8.587 ms
 Execution Time: 2718.625 ms
(19 rows)
*/


SELECT f.id_pays,id_lieu,conditions,SUM(indice_gravite) as gravite,
RANK() OVER( ORDER BY SUM(indice_gravite) DESC) as rank
FROM FAIT_ACCIDENT f NATURAL JOIN DIM_LOCALISATION JOIN DIM_METEO m 
ON (f.id_pays = m.id_pays and f.date = m.date)
GROUP BY f.id_pays,id_lieu,conditions;

-- Extract of querie output
/*  id_pays | id_lieu | conditions | gravite |  rank  
---------+---------+------------+---------+--------
       1 |  389118 | nuageux    |     141 |      1
       1 |  127282 | pluie      |     126 |      2
       2 |  903822 | pluie      |     116 |      3
       2 | 1161879 | pluie      |      89 |      4
       2 | 1346466 | pluie      |      79 |      5
       1 |  138754 | pluie      |      78 |      6
       2 | 1184790 | pluie      |      76 |      7
       1 |     929 | nuageux    |      67 |      8
       2 | 1748488 | pluie      |      66 |      9
       1 |  324077 | ensoleille |      64 |     10 */

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------
 WindowAgg  (cost=402439.75..421671.18 rows=1098939 width=31) (actual time=3331.972..10595.364 rows=1928568 loops=1)
   ->  Sort  (cost=402439.75..405187.10 rows=1098939 width=23) (actual time=3331.930..3501.285 rows=1928568 loops=1)
         Sort Key: (sum(f.indice_gravite)) DESC
         Sort Method: external merge  Disk: 65240kB
         ->  HashAggregate  (cost=245766.39..269633.97 rows=1098939 width=23) (actual time=2097.285..2844.048 rows=1928568 loops=1)
               Group Key: f.id_pays, f.id_lieu, m.conditions
               Planned Partitions: 32  Batches: 161  Memory Usage: 4153kB  Disk Usage: 95832kB
               ->  Hash Join  (cost=76076.45..161285.45 rows=1098939 width=23) (actual time=387.520..1597.752 rows=1928568 loops=1)
                     Hash Cond: ((f.id_pays = m.id_pays) AND (f.date = m.date))
                     ->  Hash Join  (cost=75843.20..155272.87 rows=1100435 width=24) (actual time=376.214..1314.843 rows=1928568 loops=1)
                           Hash Cond: ((f.id_pays = dim_localisation.id_pays) AND (f.id_lieu = dim_localisation.id_lieu))
                           ->  Seq Scan on fait_accident f  (cost=0.00..39168.68 rows=1928568 width=20) (actual time=0.025..186.822 rows=1928568 loops=1)
                           ->  Hash  (cost=39380.68..39380.68 rows=1928568 width=8) (actual time=375.321..375.323 rows=1928568 loops=1)
                                 Buckets: 131072  Batches: 32  Memory Usage: 3378kB
                                 ->  Seq Scan on dim_localisation  (cost=0.00..39380.68 rows=1928568 width=8) (actual time=0.127..141.486 rows=1928568 loops=1)
                     ->  Hash  (cost=134.70..134.70 rows=6570 width=15) (actual time=11.284..11.285 rows=6570 loops=1)
                           Buckets: 8192  Batches: 1  Memory Usage: 375kB
                           ->  Seq Scan on dim_meteo m  (cost=0.00..134.70 rows=6570 width=15) (actual time=9.575..10.327 rows=6570 loops=1)
 Planning Time: 0.348 ms
 JIT:
   Functions: 32
   Options: Inlining false, Optimization false, Expressions true, Deforming true
   Timing: Generation 1.174 ms, Inlining 0.000 ms, Optimization 0.618 ms, Emission 12.135 ms, Total 13.926 ms
 Execution Time: 10727.230 ms
(24 rows) */

-- Testing optimisation 

-- Indexes
CREATE INDEX idx_fait_id_localisation ON FAIT_ACCIDENT(id_lieu);
CREATE INDEX idx_localisation_id ON DIM_LOCALISATION(id_lieu);

DROP INDEX idx_fait_id_localisation;
DROP INDEX idx_localisation_id;

/* for q4 on localisation 
                                                                        QUERY PLAN                                                                        
----------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=270283.37..270283.39 rows=10 width=16) (actual time=2520.860..2520.865 rows=10 loops=1)
   ->  Sort  (cost=270283.37..273034.46 rows=1100435 width=16) (actual time=2515.963..2515.966 rows=10 loops=1)
         Sort Key: (sum(fait_accident.indice_gravite)) DESC
         Sort Method: top-N heapsort  Memory: 25kB
         ->  HashAggregate  (cost=224752.58..246503.36 rows=1100435 width=16) (actual time=1733.058..2384.202 rows=1928568 loops=1)
               Group Key: fait_accident.id_pays, fait_accident.id_lieu
               Planned Partitions: 32  Batches: 161  Memory Usage: 4153kB  Disk Usage: 95336kB
               ->  Hash Join  (cost=75843.20..151504.87 rows=1100435 width=16) (actual time=433.030..1312.089 rows=1928568 loops=1)
                     Hash Cond: ((fait_accident.id_pays = dim_localisation.id_pays) AND (fait_accident.id_lieu = dim_localisation.id_lieu))
                     ->  Seq Scan on fait_accident  (cost=0.00..39168.68 rows=1928568 width=16) (actual time=0.010..197.935 rows=1928568 loops=1)
                     ->  Hash  (cost=39380.68..39380.68 rows=1928568 width=8) (actual time=418.683..418.684 rows=1928568 loops=1)
                           Buckets: 131072  Batches: 32  Memory Usage: 3378kB
                           ->  Seq Scan on dim_localisation  (cost=0.00..39380.68 rows=1928568 width=8) (actual time=0.055..162.227 rows=1928568 loops=1)
 Planning Time: 0.806 ms
 JIT:
   Functions: 19
   Options: Inlining false, Optimization false, Expressions true, Deforming true
   Timing: Generation 0.860 ms, Inlining 0.000 ms, Optimization 0.454 ms, Emission 7.369 ms, Total 8.683 ms
 Execution Time: 2555.917 ms
(19 rows) */

CREATE INDEX idx_meteo_date ON DIM_METEO(date); 
CREATE INDEX idx_meteo_id_pays ON DIM_METEO(id_pays);

DROP INDEX  idx_meteo_date;
DROP INDEX idx_meteo_id_pays;

-- Tester un index composite (date, id_pays) sur DIM_METEO et mesurer l'impact
-- Q2 on top 10
/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=41921.20..41921.22 rows=10 width=19) (actual time=433.616..441.165 rows=10 loops=1)
   ->  Sort  (cost=41921.20..41945.85 rows=9861 width=19) (actual time=433.615..441.163 rows=10 loops=1)
         Sort Key: (count(*)) DESC
         Sort Method: top-N heapsort  Memory: 26kB
         ->  Finalize HashAggregate  (cost=41609.50..41708.11 rows=9861 width=19) (actual time=432.694..440.708 rows=4994 loops=1)
               Group Key: f.date, m.conditions
               Batches: 1  Memory Usage: 913kB
               ->  Gather  (cost=39390.77..41461.58 rows=19722 width=19) (actual time=428.521..437.725 rows=14540 loops=1)
                     Workers Planned: 2
                     Workers Launched: 2
                     ->  Partial HashAggregate  (cost=38390.77..38489.38 rows=9861 width=19) (actual time=397.063..397.728 rows=4847 loops=3)
                           Group Key: f.date, m.conditions
                           Batches: 1  Memory Usage: 913kB
                           Worker 0:  Batches: 1  Memory Usage: 913kB
                           Worker 1:  Batches: 1  Memory Usage: 913kB
                           ->  Hash Join  (cost=233.25..32372.19 rows=802478 width=11) (actual time=1.509..308.776 rows=642856 loops=3)
                                 Hash Cond: ((f.id_pays = m.id_pays) AND (f.date = m.date))
                                 ->  Parallel Seq Scan on fait_accident f  (cost=0.00..27918.70 rows=803570 width=8) (actual time=0.167..200.368 rows=642856 loops=3)
                                 ->  Hash  (cost=134.70..134.70 rows=6570 width=15) (actual time=1.284..1.285 rows=6570 loops=3)
                                       Buckets: 8192  Batches: 1  Memory Usage: 375kB
                                       ->  Seq Scan on dim_meteo m  (cost=0.00..134.70 rows=6570 width=15) (actual time=0.011..0.558 rows=6570 loops=3)
 Planning Time: 13.467 ms
 Execution Time: 441.284 ms
(23 rows) */

-- Q5 
/*
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 WindowAgg  (cost=402439.75..421671.18 rows=1098939 width=31) (actual time=3629.010..10654.745 rows=1928568 loops=1)
   ->  Sort  (cost=402439.75..405187.10 rows=1098939 width=23) (actual time=3628.968..3794.023 rows=1928568 loops=1)
         Sort Key: (sum(f.indice_gravite)) DESC
         Sort Method: external merge  Disk: 65240kB
         ->  HashAggregate  (cost=245766.39..269633.97 rows=1098939 width=23) (actual time=2338.824..3118.244 rows=1928568 loops=1)
               Group Key: f.id_pays, f.id_lieu, m.conditions
               Planned Partitions: 32  Batches: 161  Memory Usage: 4153kB  Disk Usage: 95832kB
               ->  Hash Join  (cost=76076.45..161285.45 rows=1098939 width=23) (actual time=404.497..1754.141 rows=1928568 loops=1)
                     Hash Cond: ((f.id_pays = m.id_pays) AND (f.date = m.date))
                     ->  Hash Join  (cost=75843.20..155272.87 rows=1100435 width=24) (actual time=393.513..1457.388 rows=1928568 loops=1)
                           Hash Cond: ((f.id_pays = dim_localisation.id_pays) AND (f.id_lieu = dim_localisation.id_lieu))
                           ->  Seq Scan on fait_accident f  (cost=0.00..39168.68 rows=1928568 width=20) (actual time=0.013..187.380 rows=1928568 loops=1)
                           ->  Hash  (cost=39380.68..39380.68 rows=1928568 width=8) (actual time=392.591..392.593 rows=1928568 loops=1)
                                 Buckets: 131072  Batches: 32  Memory Usage: 3378kB
                                 ->  Seq Scan on dim_localisation  (cost=0.00..39380.68 rows=1928568 width=8) (actual time=0.070..146.808 rows=1928568 loops=1)
                     ->  Hash  (cost=134.70..134.70 rows=6570 width=15) (actual time=10.967..10.968 rows=6570 loops=1)
                           Buckets: 8192  Batches: 1  Memory Usage: 375kB
                           ->  Seq Scan on dim_meteo m  (cost=0.00..134.70 rows=6570 width=15) (actual time=9.646..10.225 rows=6570 loops=1)
 Planning Time: 1.202 ms
 JIT:
   Functions: 32
   Options: Inlining false, Optimization false, Expressions true, Deforming true
   Timing: Generation 1.145 ms, Inlining 0.000 ms, Optimization 0.604 ms, Emission 12.063 ms, Total 13.812 ms
 Execution Time: 10768.668 ms
(24 rows)

*/