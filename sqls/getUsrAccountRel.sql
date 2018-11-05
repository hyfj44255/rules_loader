export to "${workingDir}/../csvs/usrAccountRel.csv" of del messages "${workingDir}/../logs/getUsrAccountRel_db2.log"
		SELECT
			USER_CNUM,
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
				WHERE
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
					A.CCMS_ID,
					U.EMPLOYEE_CNUM,
					a.CCMS_LEVEL
			)
			WHERE
	        CCMS_ID IS NOT NULL
	        AND trim(CCMS_ID) <> ''
	        WITH UR;

