import from "${workingDir}/../csvs/smr2CmrPrdct.csv" of del METHOD P (1,2,3,4,5) commitcount 10000  messages "${workingDir}/../logs/impSmr2CmrPrdct_db2.log" INSERT into CMRDC.${stagingTable}  ( MPP_NUM, PRODUCT_ID, USER_CNUM,COUNTRY,PRODUCT_LEVEL);
--import from "${workingDir}/smr2CmrPrdct.csv" of del METHOD P (1,2,3,4,5)  messages "${workingDir}/impSmr2CmrPrdct_db2.log" INSERT into SCTID.${stagingTable}  ( MPP_NUM, PRODUCT_ID, USER_CNUM,COUNTRY,PRODUCT_LEVEL);