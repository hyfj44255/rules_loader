--export to "${workingDir}/../csvs/dcCmrCcms.csv" of del messages "${workingDir}/../logs/getDcCmrCcms_db2.log"
insert into CMRDC.${dcCmrCcms} (DC_CCMS, CMR_CCMS, MPP_NUM, COUNTRY)
SELECT
	A2.CCMS_ID AS dc_ccms,
	A1.CCMS_ID AS cmr_ccms,
	ac.mpp_num,
	Ac.landed_country
--    CASE
--    WHEN LENGTH(
--      TRIM( Ac.landed_country )
--    )= 0 THEN 'WW'
--    ELSE Ac.landed_country
--    END AS Country
  FROM
	(
		SELECT
			in_A1.CCMS_ID,
			in_A1.PARENT_ID,
			in_A1.id
		FROM
			SME.ACCOUNTS in_A1
		WHERE
			in_A1.CCMS_LEVEL = 'S'
			AND in_A1.DELETED = 0
	) AS a1
INNER JOIN SME.ACCOUNTS A2 ON
	A1.PARENT_ID = A2.ID
	AND A2.DELETED = 0
INNER JOIN(
		SELECT
			aui.ACCOUNT_ID
		FROM
			SCTID.ACCOUNTS_USERS AUi
		INNER JOIN SCTID.USERS U ON
			U.ID = AUi.USER_ID
			AND U.DELETED = 0
		WHERE
			AUi.DELETED = 0
		GROUP BY
			aui.ACCOUNT_ID
	) AU ON
	AU.ACCOUNT_ID = A1.ID --LEFT JOIN
LEFT JOIN SCTID.accounts_cstm ac ON
	a1.id = ac.id_c and length(trim(ac.mpp_num)) <>0
fetch first 100 rows only
WITH ur;