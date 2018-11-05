export to "${workingDir}/../csvs/usrPrdctRel.csv" of del messages "${workingDir}/../logs/getUsrPrdctRel_db2.log"
SELECT
	(
		SELECT
			innusr.EMPLOYEE_CNUM
		FROM
			SCTID.USERS innusr
		WHERE
			innusr.ID = up.user_ID
			AND innusr.DELETED = 0
	) AS user_CNUM,
	up.PRODUCT_ID AS PRODUCT_ID,
	up.PRODUCT_LEVEL AS PRODUCT_LEVEL
FROM
	SCTID.IBM_USERS_PRODUCTS up
WHERE
	up.DELETED = 0
	and EXISTS(
		SELECT
			1
		FROM
			SCTID.IBM_NODE_USERS AS NU
		INNER JOIN SCTID.USERS U ON
			NU.ASSIGNED_USER_ID = U.ID
		INNER JOIN SCTID.ACCOUNTS_USERS AU ON
			U.ID = AU.USER_ID
		WHERE
			u.id = up.USER_ID --over here!!
			and
			(
				NU.USER_TYPE = 'OWNER'
				OR NU.USER_TYPE = 'ROOWNER'
			)
			AND NU.DELETED = 0
			AND U.DELETED = 0
			AND AU.DELETED = 0
			AND u.STATUS = 'Active'
			group by u.id
	) WITH ur;