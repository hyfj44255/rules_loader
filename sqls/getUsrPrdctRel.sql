export to "${workingDir}/../csvs/usrPrdctRel.csv" of del messages "${workingDir}/../logs/getUsrPrdctRel_db2.log"
SELECT
(SELECT EMPLOYEE_CNUM FROM SCTID.USERS WHERE ID=user_ID AND DELETED=0) AS user_CNUM,
PRODUCT_ID AS PRODUCT_ID,
PRODUCT_LEVEL AS PRODUCT_LEVEL
FROM SCTID.IBM_USERS_PRODUCTS WHERE DELETED=0
AND USER_ID IN (
SELECT
distinct u.id AS use_id
FROM
  SCTID.IBM_NODE_USERS AS NU --user node
INNER JOIN SCTID.USERS U ON
  NU.ASSIGNED_USER_ID = U.ID
INNER JOIN SCTID.ACCOUNTS_USERS AU ON --user client team  accountid mutil user
  U.ID = AU.USER_ID
INNER JOIN sme.ACCOUNTS A ON
  AU.ACCOUNT_ID = A.ID AND A.ccms_level = 'S'
--LEFT JOIN IBM_USERS_PRODUCTS UP ON --user sell product
--  U.ID = up.USER_ID
left join SCTID.accounts_cstm AS ac ON
    ac.id_c = a.id
WHERE
  (
    NU.USER_TYPE = 'OWNER' -- user  node
    OR NU.USER_TYPE = 'ROOWNER'--user type : readonly owner
  )
  AND NU.DELETED = 0
  AND U.DELETED = 0
  AND AU.DELETED = 0
  AND A.DELETED = 0
--  AND UP.DELETED = 0
  --AND U.USER_JOB_ROLE_SKILL_SET = 'Security Services'
  AND u.STATUS = 'Active'
AND  ac.mpp_num IS not NULL
AND ac.mpp_num <>''
AND ac.landed_country <>''
)
WITH ur;