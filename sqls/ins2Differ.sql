INSERT INTO CMRDC.RULES_DATA_DIFF --加个编辑时间  (改状态时候)
(ACTION ,MPP_NUM ,PRODUCT_ID ,USER_CNUM  ,
COUNTRY ,PRODUCT_LEVEL  ,STATUS_CODE)
SELECT 
--sctid.newguid AS id,
ACTION,
CASE WHEN tt.ACTION ='D'  THEN
	tt.rd_mppnum
ELSE
 	tt.rds_mppnum
END AS mpp_num,

CASE WHEN tt.ACTION ='D'  THEN
	tt.rd_proid
ELSE
 	tt.rds_proid
END AS PRODUCT_ID,

CASE WHEN tt.ACTION ='D'  THEN
	tt.rd_ucnum
ELSE
 	tt.rds_ucnum
END AS USER_CNUM,
CASE WHEN tt.ACTION ='D'  THEN
	tt.rd_country
ELSE
 	tt.rds_country
END AS COUNTRY,
CASE WHEN tt.ACTION ='D'  THEN
	tt.rd_prol
ELSE
 	tt.rds_prol
END AS PRODUCT_LEVEL,
0
FROM 
(
SELECT
	rd.ENTERED_TIME AS rdid ,
	rds.ENTERED_TIME  rdsid ,
	CASE
		WHEN rds.ENTERED_TIME IS NULL
		AND rd.ENTERED_TIME IS NOT NULL THEN 'D'
		WHEN rds.ENTERED_TIME IS NOT NULL
		AND rd.ENTERED_TIME IS NULL then 'I'
	END AS ACTION,
	rd.MPP_NUM AS rd_mppnum,
	rd.PRODUCT_ID AS rd_proid,
	rd.USER_CNUM AS rd_ucnum,
	rd.COUNTRY AS rd_country,
	rd.PRODUCT_LEVEL AS rd_prol,
	rds.MPP_NUM AS rds_mppnum,
	rds.PRODUCT_ID AS rds_proid,
	rds.USER_CNUM AS rds_ucnum,
	rds.COUNTRY AS rds_country,
	rds.PRODUCT_LEVEL AS rds_prol
FROM
	CMRDC.${stagingTable} rds --RULES_DATA_STAGING1
FULL JOIN CMRDC.${rulesDataTable} rd ON --RULES_DATA1
	(
		rds.MPP_NUM = rd.MPP_NUM
		AND rds.PRODUCT_ID = rd.PRODUCT_ID
		AND rds.USER_CNUM = rd.USER_CNUM
		--看看conutry空情况下 mppnumber是否唯一
		AND (rds.COUNTRY = rd.COUNTRY OR (rds.COUNTRY IS null AND rd.COUNTRY IS NULL))
		--country为空可否接受
		AND rds.PRODUCT_LEVEL = rd.PRODUCT_LEVEL
	)
) tt WHERE tt.rdid IS NULL OR tt.rdsid IS NULL;

UPDATE SCTID.RULES_DATA_DIFF SET STATUS_CODE = 2 ,LAST_UPDATING_DATE = current timestamp where STATUS_CODE = 0; 
