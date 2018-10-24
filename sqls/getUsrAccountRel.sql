export to "${workingDir}/../csvs/usrAccountRel.csv" of del messages "${workingDir}/../logs/getUsrAccountRel_db2.log"
SELECT
	USER_CNUM,
	CCMS_ID,
	CCMS_LEVEL
FROM
	(
		SELECT
			DISTINCT USER_CNUM,
			CCMS_ID,
			CCMS_LEVEL
		FROM
			(
				SELECT
					U.EMPLOYEE_CNUM AS USER_CNUM,
					A.CCMS_ID,
					A.CCMS_LEVEL
				FROM
					SME.IBM_NODE_USERS AS NU
				INNER JOIN SME.USERS U ON
					NU.ASSIGNED_USER_ID = U.ID
				INNER JOIN SME.ACCOUNTS_USERS AU ON
					U.ID = AU.USER_ID
				INNER JOIN SME.ACCOUNTS A ON
					AU.ACCOUNT_ID = A.ID
					--AND A.CCMS_LEVEL = 'S'
				WHERE
					(
						NU.USER_TYPE = 'OWNER' -- USER ÓÐÎÞ NODE
						OR NU.USER_TYPE = 'ROOWNER' --USER TYPE : READONLY OWNER
					)
					AND NU.DELETED = 0
					AND U.DELETED = 0
					AND AU.DELETED = 0
					AND A.DELETED = 0 --AND U.USER_JOB_ROLE_SKILL_SET = 'SECURITY SERVICES'
					AND U.STATUS = 'ACTIVE'
				GROUP BY
					A.CCMS_ID,
					U.EMPLOYEE_CNUM,
					CCMS_LEVEL
			)
	)
WHERE
	CCMS_ID IS NOT NULL
	AND CCMS_ID <> ''
	fetch first 100 rows only
	WITH UR;