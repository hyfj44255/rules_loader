export to "${workingDir}/../csvs/dcCmrCcms.csv" of del messages "${workingDir}/../logs/getDcCmrCcms_db2.log"
	SELECT
	A2.CCMS_ID AS dc_ccms,
	A1.CCMS_ID AS cmr_ccms,
	ac.mpp_num,
	Ac.landed_country
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
			and in_A1.CCMS_ID is not null
			and TRIM(in_A1.CCMS_ID) <> ''
	) AS a1
LEFT JOIN SME.ACCOUNTS A2 ON
	A1.PARENT_ID = A2.ID
	AND A2.DELETED = 0
LEFT JOIN SCTID.accounts_cstm ac ON
	a1.id = ac.id_c
WHERE
EXISTS (
		SELECT
			1
		FROM
			SME.IBM_NODE_USERS AS NU
		INNER JOIN SME.USERS U ON
			NU.ASSIGNED_USER_ID = U.ID
		INNER JOIN SME.ACCOUNTS_USERS AU ON
			U.ID = AU.USER_ID
		INNER JOIN SME.ACCOUNTS A ON
			AU.ACCOUNT_ID = A.ID
		WHERE
			a.id = a1.id -- over here!
			AND
			(
				NU.USER_TYPE = 'OWNER'
				OR NU.USER_TYPE = 'ROOWNER'
			)
			AND NU.DELETED = 0
			AND U.DELETED = 0
			AND AU.DELETED = 0
			AND A.DELETED = 0
			AND U.STATUS = 'Active'
		GROUP BY
			A.CCMS_ID
			)
	WITH ur;