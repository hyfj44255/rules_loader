export to "${workingDir}/../csvs/usrAccountRel.csv" of del messages "${workingDir}/../logs/getUsrAccountRel_db2.log"
SELECT user_cnum,mpp_num,country
--SELECT count(*)
FROM (
SELECT DISTINCT user_cnum AS user_cnum, mpp_num AS mpp_num, country AS country FROM
(
SELECT
  --sctid.newguid,
  ac.MPP_NUM AS MPP_NUM,-- account not mpp_num
--  CASE
--    WHEN product_id IS NULL THEN '*'
--    ELSE UP.PRODUCT_ID
--  END Product_ID,
  U.EMPLOYEE_CNUM AS User_CNUM,
  --CURRENT TIMESTAMP AS intime,
  case
    when length(trim(Ac.landed_country)) = 0 then 'WW'
    else Ac.landed_country
  end AS Country
--  CASE
--    WHEN UP.PRODUCT_LEVEL IS NULL THEN 0
--    ELSE UP.PRODUCT_LEVEL
--  END PRODUCT_LEVEL
FROM
  sme.IBM_NODE_USERS AS NU --user node
INNER JOIN sme.USERS U ON
  NU.ASSIGNED_USER_ID = U.ID
INNER JOIN sme.ACCOUNTS_USERS AU ON --user client team  accountid mutil user
  U.ID = AU.USER_ID
INNER JOIN sme.ACCOUNTS A ON
  AU.ACCOUNT_ID = A.ID AND A.ccms_level = 'S' --Ö»È¡siteµÄÖµ ¼Ó¹ýÂË
--LEFT JOIN sme.IBM_USERS_PRODUCTS UP ON --user sell product
--  U.ID = up.USER_ID and UP.DELETED = 0
left join sme.accounts_cstm AS ac ON
    ac.id_c = a.id
WHERE
  (
    NU.USER_TYPE = 'OWNER' -- user ÓÐÎÞ node
    OR NU.USER_TYPE = 'ROOWNER'--user type : readonly owner
  )
  AND NU.DELETED = 0
  AND U.DELETED = 0
  AND AU.DELETED = 0
  AND A.DELETED = 0
  --AND UP.DELETED = 0
  AND U.USER_JOB_ROLE_SKILL_SET = 'Security Services' --250ÈËÐèÒªÈ·ÈÏ
  AND u.STATUS = 'Active'
  and ac.MPP_NUM is not null
  GROUP BY  ac.MPP_NUM,--UP.PRODUCT_ID,
  U.EMPLOYEE_CNUM,Ac.landed_country --,UP.PRODUCT_LEVEL;
 ))
WHERE mpp_num IS not NULL
AND mpp_num <>''
AND country <>''
with ur;