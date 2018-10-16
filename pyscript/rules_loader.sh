#!/bin/bash

function sendEmail
{ # msg1
    echo "message: $1 "
}

function sendMessageAndExit
{ # msg, *cleanTmpFile
    msg=$1;
    # times=0;
    # sendEmail ${msg}
    # for i in "$@"; do
    #     if [[ $times == 0 ]];
    #     then
    #         echo $i
    #         times=1
    #     fi
    # done
    echo $msg
    exit 1
}

function execCommand
{
    action=$1
    command=$2
    nowtime=`date`
    echo $action' start at '$nowtime
    starttime=$(date +%s)
    eval $command
    res=$?
    # echo $action' res= '$res
    endtime=$(date +%s)
    nowtime=`date`
    echo $action' end at '$nowtime
    cost=$((endtime - starttime))
    echo $action' costs '$cost' seconds'
    return $res
}

# self, sourceSql, sedDiect
function sql2TmpSql
{
    sqlPath=$workingPath"/../sqls/"
    sourceSql=$1
    paraKey=$2
    paraKey=(${paraKey// / })
    paraVal=$3
    paraVal=(${paraVal// / })
    command="cat "${sqlPath}${sourceSql}
    for((i=0;i<${#paraKey[@]};i++)); do #${paraKey[$i]}
        command=$command' | sed '"'"'s/${'
        command=$command${paraKey[$i]}"}/"${paraVal[$i]}'/g'"'"
    done
    command=$command' > '${sqlPath}'tmp_'${sourceSql}
    eval $command
}

function generateTmpSql
{
    # declare -A map1=(["workingDir"]=spDir)
    # declare -A map2=(["workingDir"]=spDir)
    # declare -A map3=(["workingDir"]=spDir)
    # declare -A map4=(["workingDir"]=spDir ["stagingTable"]=containStaging)
    # declare -A map5=(["stagingTable"]=containStaging ["rulesDataTable"]=containData)
    # declare -A map6=(["stagingTable"]=containStaging)
    # declare -A map7=(["stagingTable"]=containStaging)
    # declare -A map8=()

    para1key=('workingDir')
    para1val=($spDir)

    para2key=('workingDir' 'stagingTable')
    para2val=($spDir $containStaging)

    para3key=('stagingTable' 'rulesDataTable')
    para3val=($containStaging $containData)

    para4key=('stagingTable')
    para4val=($containStaging)


    #smrTmpSql
    sql2TmpSql 'smr2CmrPrdct.sql' "${para1key[*]}" "${para1val[*]}"
    sql2TmpSql 'getProduct.sql' "${para1key[*]}" "${para1val[*]}" #productTmpSql

    sql2TmpSql 'getUsrAccountRel.sql' "${para1key[*]}" "${para1val[*]}" #new
    sql2TmpSql 'getUsrPrdctRel.sql' "${para1key[*]}" "${para1val[*]}" #new

    sql2TmpSql 'impProduct.sql' "${para1key[*]}" "${para1val[*]}" #impProductTmpSql

    sql2TmpSql 'impUsrAccountRel.sql' "${para1key[*]}" "${para1val[*]}" #new
    sql2TmpSql 'impUsrPrdctRel.sql' "${para1key[*]}" "${para1val[*]}" #new

    sql2TmpSql 'impSmr2CmrPrdct.sql' "${para2key[*]}" "${para2val[*]}" #impSmrTmpSql
    sql2TmpSql 'ins2Differ.sql' "${para3key[*]}" "${para3val[*]}" #diffTmpSql
    sql2TmpSql 'smrImpFailed.sql' "${para4key[*]}" "${para4val[*]}" #impSmrFailTmpSql
    sql2TmpSql 'space4StagePrdct.sql' "${para4key[*]}" "${para4val[*]}" #space4StagePrdct
    sql2TmpSql 'impPrductFailed.sql' #impPdctFailTmpSql
}

function dbExecCommand
{
    #(sqlFile, logsPath='', logName=''):
    sqlFile=$1
    logsPath=$2
    logName=$3
    sqlPath=$workingPath"/../sqls/"
    var="db2 -tvf "${sqlPath}"tmp_"${sqlFile}
    if [ -n ${logsPath} ] && [ -n ${logName} ]; then
        var=$var" > "${logsPath}${logName}
    fi
    echo $var
}


function ReadINIfile
{
    INIFILE=$1
    SECTION=$2
    ITEM=$3
    ReadINI=`awk -F '=' '/\['$SECTION'\]/{a=1}a==1&&$1~/'$ITEM'/{print $2;exit}' $INIFILE`
    tablesPhases=$ReadINI
    export tablesPhases
}

# ./ini.sh ./../dbDriver.properties [1] tablesPhases
function WriteINIfile
{
    INIFILE=$1
    SECTION=$2
    ITEM=$3
    NEWVAL=$4
    # awk -F '=' '/\['${section}'\]/{a=1} (a==1 && "'${item}'"==$1){gsub($2,"\"'${val}'\"");a=0} {print $0}' ${file} 1<>${file}
    awk -F '=' '/\['${SECTION}'\]/{a=1} (a==1 && "'${ITEM}'"==$1){gsub($2,"'${NEWVAL}'");a=0} {print $0}' ${INIFILE} 1<>${INIFILE}
}

workingPath=`dirname $0`
readonly workingPath
properFile=$workingPath'/../dbDriver.properties'

ReadINIfile $properFile '[1]' 'tablesPhases'
echo 'tablesPhases after invoked='$tablesPhases

nodeName=SVT6 #$nodeName
dbIp=9.37.16.2 #$dbIp
port=50000 #$port
alia=SVT6_DB #$alia
usr=sctid #$usr
pwd=may15pwd #$pwd
dbInstance=SALECONN #$dbInstance

ourNodeName=ICP #$ourNodeName
ourDbIp=9.32.165.190 #$ourDbIp
ourPort=31372 #$ourPort
ourAlia=ICP_DB #$ourAlia
ourUsr=a1insctp #$ourUsr
ourPwd=Aug18pwd #$ourPwd
ourDbInstance=ICP_DB #$ourDbInstance
dbServerPort=22 #$dbServerPort
dbServerUser=a1insctp #$dbServerUser
dbServerPwd=12345678 #$dbServerPwd
needUncataDbAlia=0 #$needUncataDbAlia
needUncataDbNode=0 #$needUncataDbNode

if [[ $tablesPhases == 0 ]]; then
    containStaging='RULES_DATA_STAGING'
    containData='RULES_DATA'
    containSS='RULES_DATA_SS'
fi

if [[ $tablesPhases == 1 ]]; then
    containStaging='RULES_DATA_SS'
    containData='RULES_DATA_STAGING'
    containSS='RULES_DATA'
fi

if [[ $tablesPhases == 2 ]]; then
    containStaging='RULES_DATA'
    containData='RULES_DATA_SS'
    containSS='RULES_DATA_STAGING'
fi

spDir=${workingPath//\//\\/}
logsPath=$workingPath'/../logs/'
csvFloder=$workingPath'/../csvs/'
csvFile=$workingPath'/../csvs/smr2CmrPrdct.csv'
sqlScript=$workingPath'/../sqls/tmp_impSmr2CmrPrdct.sql'
productCsvFile=$workingPath'/../csvs/productsData.csv'
productSqlScript=$workingPath'/../sqls/tmp_impProduct.sql'
sourceAixEnv='. /etc/profile && . /home/db2inst1/.profile'


dbAlia='db2 catalog db %s as %s at node %s'
dbNode='db2 catalog tcpip node %s remote %s server %s remote_instance %s'
conn2Db='db2 connect to %s user %s using %s'
isNodeExist='db2 list node directory | grep -i %s'
isAliaExist='db2 list database directory | grep -i %s'
uncataDBAlia='db2 uncatalog db %s'
uncataNode='db2 uncatalog node %s'
connOuterSpaceDb='db2 connect to %s user %s using %s'

remoteDbAlia=`printf "$dbAlia" $dbInstance $alia $nodeName`
remoteDBNode=`printf "$dbNode" $nodeName $dbIp $port $dbInstance`
conn2RemoteDb=`printf "$conn2Db" $alia $usr $pwd`
ourDbAlia=`printf "$dbAlia" $ourDbInstance $ourAlia $ourNodeName`
ourDBNode=`printf "$dbNode" $ourNodeName $ourDbIp $ourPort $ourDbInstance`
conn2OurDb2=`printf "$conn2Db" $ourAlia $ourUsr $ourPwd`
isNodeExist=`printf "$isNodeExist" $nodeName`
isAliaExist=`printf "$isAliaExist" $alia`
isOurNodeExist=`printf "$isNodeExist" $ourNodeName`
isOurAliaExist=`printf "$isAliaExist" $ourAlia`
uncataDBAlia=`printf "$uncataDBAlia" $alia`
uncataNode=`printf "$uncataNode" $nodeName`
uncataOurDBAlia=`printf "$uncataDBAlia" $ourAlia`
uncataOurNode=`printf "$uncataNode" $ourNodeName`
connOuterSpaceDb=`printf "$connOuterSpaceDb" $ourDbInstance $ourUsr $ourPwd`

generateTmpSql

# function generateExecCommand
logsPath=$workingPath'/../logs/'
sqlPath=$workingPath"/../sqls/"
# getSmr=`dbExecCommand smr2CmrPrdct.sql $logsPath smr2CmrPrdct.log`
getSmr="db2 -tvf "${sqlPath}"tmp_smr2CmrPrdct.sql > "${logsPath}"smr2CmrPrdct.log"
# dbExecCommand space4StagePrdct.sql $logsPath space4StagePrdct.log
makeSpace="db2 -tvf "${sqlPath}"tmp_space4StagePrdct.sql > "${logsPath}"space4StagePrdct.log"
# dbExecCommand ins2Differ.sql
# ins2Differ=''
# dbExecCommand smrImpFailed.sql $logsPath smrImpFailed.log
smrImpFail="db2 -tvf "${sqlPath}"tmp_smrImpFailed.sql > "${logsPath}"smrImpFailed.log"
# dbExecCommand getProduct.sql $logsPath getProduct.log
productsData="db2 -tvf "${sqlPath}"tmp_getProduct.sql > "${logsPath}"getProduct.log"

usrPrdctRelData="db2 -tvf "${sqlPath}"tmp_getUsrPrdctRel.sql > "${logsPath}"getUsrPrdctRel.log" #new
usrAccountRelData="db2 -tvf "${sqlPath}"tmp_getUsrAccountRel.sql > "${logsPath}"getUsrAccountRel.log" #new

# dbExecCommand impPrductFailed.sql $logsPath impPrductFailed.log
impPrdctFaild="db2 -tvf "${sqlPath}"tmp_impPrductFailed.sql > "${logsPath}"impPrductFailed.log"
# dbExecCommand impSmr2CmrPrdct.sql
impSourceCSVLocal="db2 -tvf "${sqlPath}"tmp_impSmr2CmrPrdct.sql"
# dbExecCommand impProduct.sql
importProductLocal="db2 -tvf "${sqlPath}"tmp_impProduct.sql"

impUsrAccountRel="db2 -tvf "${sqlPath}"tmp_impUsrAccountRel.sql" #new
impUsrPrdctRel="db2 -tvf "${sqlPath}"tmp_impUsrPrdctRel.sql" #new


$conn2RemoteDb
if [[ $? -ne 0 ]]; then
    sendMessageAndExit 'conn2SCdb failed'
fi

execCommand 'productsData' "${productsData}"
if [[ $? -ne 0 ]]; then
    sendMessageAndExit 'getProductsData failed'
fi

#execCommand 'usrPrdctRelData' "${usrPrdctRelData}"
#if [[ $? -ne 0 ]]; then
#    sendMessageAndExit 'getUsrPrdctRelData failed'
#fi
#
#execCommand 'usrAccountRelData' "${usrAccountRelData}"
#if [[ $? -ne 0 ]]; then
#    sendMessageAndExit 'getUsrAccountRelData failed'
#fi

 execCommand 'getSmr' "${getSmr}"
 if [[ $? -ne 0 ]]; then
     sendMessageAndExit 'getSmr failed'
 fi
# echo $conn2OurDb2
$conn2OurDb2
if [[ $? -ne 0 ]]; then
    sendMessageAndExit 'conn2OurDb2 failed'
fi

execCommand 'makeSpace' "${makeSpace}"
if [[ $? -ne 0 ]]; then
    sendMessageAndExit 'makeSpace failed'
fi
 execCommand 'impSourceCSVLocal' "${impSourceCSVLocal}"
 if [[ $? -ne 0 ]]; then
     execCommand 'cleanRDTable' "${smrImpFail}"
     sendMessageAndExit 'impSmrCSV failed'
 fi
execCommand 'importProductLocal' "${importProductLocal}"
if [[ $? -ne 0 ]]; then
    execCommand 'cleanProductTable' "${impPrdctFaild}"
    sendMessageAndExit 'importProductLocal failed'
fi

#execCommand 'impUsrAccountRel' "${impUsrAccountRel}"
#if [[ $? -ne 0 ]]; then
#    sendMessageAndExit 'impUsrAccountRel failed'
#fi
#
#execCommand 'impUsrPrdctRel' "${impUsrPrdctRel}"
#if [[ $? -ne 0 ]]; then
#    sendMessageAndExit 'impUsrPrdctRel failed'
#fi

# eval $makeSpace
# eval $impSourceCSVLocal
# eval $importProductLocal
if [[ $tablesPhases = 2 ]]; then
    tablesPhases=0
else
    tablesPhases=`expr $tablesPhases + 1`
fi
echo 'after add='$tablesPhases
WriteINIfile $properFile '[1]' 'tablesPhases' $tablesPhases

ReadINIfile $properFile '[1]' 'tablesPhases'
echo 'tablesPhases after invoked='$tablesPhases

#request_url='http://9.115.158.70:5000/v1/lineItemTeam/ '
#basic_auth=`echo -n 'admin:asdf' | base64`
#request_post='curl --request POST --url '$request_url
#request_body='{ "payload": [   {     "rli_id": "newrli1",     "account_mpp": "0043389592",     "account_country": "DE",     "prod_id": "B7M60"   },   {     "rli_id": "newrli2",     "account_mpp": "0016557777",     "account_country": "US",     "prod_id": "B7MB7"   },   {     "rli_id": "newrli3",     "account_mpp": "0031053049",     "account_country": "US",     "prod_id": "B7M60"   },   {     "rli_id": "newrli4",     "account_mpp": "0043389592",     "account_country": "CN",     "prod_id": "B7M60"   },   {     "rli_id": "newrli5",     "account_mpp": "0043389592",     "account_country": "DE",     "prod_id": "B7AM0"   },   {     "rli_id": "newrli6",     "account_mpp": "0042559994",     "account_country": "US",     "prod_id": "*"   },   {     "rli_id": "newrli7",     "account_mpp": "0031053049",     "account_country": "US",     "prod_id": "*"   } ], "timestamp": "2018-09-05 01:46:15.914"}'
#request_post=$request_post" --header 'Content-Type: application/json'"
#request_post=$request_post" --header 'Authorization: Basic ${basic_auth}'"
#request_post=$request_post" --header 'cache-control: no-cache' "
#request_post=$request_post' --data '"'"${request_body}"'"
#eval ${request_post}