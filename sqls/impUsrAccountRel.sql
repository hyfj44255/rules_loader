import from "${workingDir}/../csvs/usrAccountRel.csv" of del  METHOD P ( 1,2,3,4 ) messages "${workingDir}/../logs/impUsrAccountRel_db2.log" INSERT into CMRDC.USER_ACCOUNT_REL (USER_CNUM ,MPP_NUM ,PRODUCT_ID ,COUNTRY);
--import from "${workingDir}/productsData.csv" of del  METHOD P ( 1,2,3,4,5,6 ) messages "${workingDir}/impProductCsv_db2.log" INSERT into SCTID.PRODUCTS_IMAGE (NAME ,LEV30 ,LEV20 ,LEV17 ,LEV15 ,LEV10);