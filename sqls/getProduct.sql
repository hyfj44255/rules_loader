export to "${workingDir}/../csvs/productsData.csv" of del messages "${workingDir}/../logs/getProduct_db2.log"
SELECT
  --sctid.newguid id_pk,
  t1.NAME,
  t1.id AS lev30,
  t2.id AS lev20,
  t2.LEVEL17 AS lev17,
  t3.id AS lev15,
  t4.id AS lev10
FROM
  sme.IBM_PRODUCTS t1 
LEFT JOIN sme.IBM_PRODUCTS t2
 ON
  t1.PARENT_ID = t2.ID
  AND t2.DELETED = 0
  AND t2.ACTIVITY_STATUS = 'Active'
  AND t2.TYPE = 'product'
LEFT JOIN sme.IBM_PRODUCTS t3
 ON
  t2.PARENT_ID = t3.ID
  AND t3.DELETED = 0
  AND t3.ACTIVITY_STATUS = 'Active'
  AND t3.TYPE = 'product'
LEFT JOIN sme.IBM_PRODUCTS t4
 ON
  t3.PARENT_ID = t4.ID
  AND t4.DELETED = 0
  AND t4.ACTIVITY_STATUS = 'Active'
  AND t4.TYPE = 'product'
WHERE
  t1.LEVEL = 30
  AND t1.DELETED = 0
  AND t1.ACTIVITY_STATUS = 'Active'
  AND t1.TYPE = 'product'
  with ur;