
--TRUNCATE TABLE SCTID.${rulesDataTable} IMMEDIATE;

-- INSERT INTO SCTID.${rulesDataTable}
-- (MPP_NUM ,PRODUCT_ID ,USER_CNUM ,ENTERED_TIME ,COUNTRY ,PRODUCT_LEVEL)
-- SELECT 
-- MPP_NUM,PRODUCT_ID,USER_CNUM,
-- ENTERED_TIME,COUNTRY,PRODUCT_LEVEL 
-- FROM SCTID.${stagingTable};