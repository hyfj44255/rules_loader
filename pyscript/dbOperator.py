#!/usr/bin/python
# -*- coding: UTF-8 -*-

import subprocess
import time
from contextInitiator import ContextInitiator
from dbHelper import DBHelper
import dbHelper
import sshClient
from sshClient import SSHClient
import sys


class DBOperator(object):
    """docstring for DBOperator"""
    __spDir = ''
    __spDir4Outer = ''
    __containStaging = ''
    __containData = ''
    __containSS = ''
    __logsPath = ''

    __getSmr = ''
    __makeSpace = ''
    __ins2Differ = ''
    # __backupSnyc = ''
    __smrImpFail = ''
    __productsData = ''
    __impPrdctFaild = ''
    __impSourceCSV = ''
    __importProduct = ''

    __impSourceCSVLocal = ''
    __importProductLocal = ''

    __remoteDbAlia = ''
    __remoteDBNode = ''
    __conn2RemoteDb = ''
    __connOuterSpaceDb = ''

    __ourDbAlia = ''
    __ourDBNode = ''
    __conn2OurDb2 = ''

    __isNodeExist = ''
    __isAliaExist = ''

    __isOurNodeExist = ''
    __isOurAliaExist = ''

    __uncataDBAlia = ''
    __uncataNode = ''
    __uncataOurDBAlia = ''
    __uncataOurNode = ''
    __csvFile = ''
    __sqlScript = ''
    __productCsvFile = ''
    __productSqlScript = ''
    __sshCli = None
    __sourceAixEnv = ''

    def __init__(self):
        super(DBOperator, self).__init__()
        context = ContextInitiator()
        workingpath = context.getWorkingPath()
        self.__spDir = workingpath.replace("/", "\/")
        dbWorkingFloder = context.getProperDict()['dbWorkingFloder']
        self.__spDir4Outer = dbWorkingFloder.replace("/", "\/")
        self.__containStaging = context.getContainStaging()
        self.__containData = context.getContainData()
        self.__containSS = context.getContainSS()
        self.__logsPath = workingpath + "/../logs/"
        self.__csvFloder = workingpath + "/../csvs/"
        self.__csvFile = workingpath + "/../csvs/smr2CmrPrdct.csv"
        self.__sqlScript = workingpath + "/../sqls/tmp_impSmr2CmrPrdct.sql"
        self.__productCsvFile = workingpath + "/../csvs/productsData.csv"
        self.__productSqlScript = workingpath + "/../sqls/tmp_impProduct.sql"
        self.__sourceAixEnv = '. /etc/profile && . /home/db2inst1/.profile'

    def __DBConnStr(self):
        dicty = ContextInitiator().getProperDict()
        db = DBHelper()
        self.__remoteDbAlia = db.getDbAliaCmd((dicty['dbInstance'], dicty['alia'], dicty['nodeName']))
        self.__remoteDBNode = db.getDbNodeCmd((dicty['nodeName'], dicty['dbIp'], dicty['port'], dicty['dbInstance'],))
        self.__conn2RemoteDb = db.getConn2DbCmd((dicty['alia'], dicty['usr'], dicty['pwd']))

        self.__ourDbAlia = db.getDbAliaCmd((dicty['ourDbInstance'], dicty['ourAlia'], dicty['ourNodeName']))
        self.__ourDBNode = db.getDbNodeCmd(
            (dicty['ourNodeName'], dicty['ourDbIp'], dicty['ourPort'], dicty['ourDbInstance']))
        self.__conn2OurDb2 = db.getConn2DbCmd((dicty['ourAlia'], dicty['ourUsr'], dicty['ourPwd']))

        self.__isNodeExist = db.isNodeExistCmd((dicty['nodeName']))
        self.__isAliaExist = db.isAliaExistCmd((dicty['alia']))  # .upper()

        self.__isOurNodeExist = db.isNodeExistCmd((dicty['ourNodeName']))
        self.__isOurAliaExist = db.isAliaExistCmd((dicty['ourAlia']))  # .upper()

        self.__uncataDBAlia = db.getUncataDBAliaCmd((dicty['alia']))
        self.__uncataNode = db.getUncataNode((dicty['nodeName']))
        self.__uncataOurDBAlia = db.getUncataDBAliaCmd((dicty['ourAlia']))
        self.__uncataOurNode = db.getUncataNode((dicty['ourNodeName']))
        self.__connOuterSpaceDb = db.getOuterSpaceDbCmd((dicty['ourDbInstance'], dicty['ourUsr'], dicty['ourPwd']))

    def __generateTmpSql(self):
        ex = ''
        try:
            db = DBHelper()
            smrTmpSql = db.sql2TmpSql("smr2CmrPrdct.sql", {"workingDir": self.__spDir})
            productTmpSql = db.sql2TmpSql("getProduct.sql", {"workingDir": self.__spDir})

            # impProductTmpSql = db.sql2TmpSql("impProduct.sql",{"workingDir":self.__spDir4Outer})
            # impSmrTmpSql = db.sql2TmpSql("impSmr2CmrPrdct.sql",{"workingDir":self.__spDir4Outer,"stagingTable":self.__containStaging})

            impProductTmpSql = db.sql2TmpSql("impProduct.sql", {"workingDir": self.__spDir})
            impSmrTmpSql = db.sql2TmpSql("impSmr2CmrPrdct.sql",
                                         {"workingDir": self.__spDir, "stagingTable": self.__containStaging})

            diffTmpSql = db.sql2TmpSql("ins2Differ.sql",
                                       {"stagingTable": self.__containStaging, "rulesDataTable": self.__containData})
            # buckupTmpSql = db.sql2TmpSql("backupSnyc.sql",{"stagingTable":self.__containStaging,"rulesDataTable":self.__containData})
            impSmrFailTmpSql = db.sql2TmpSql("smrImpFailed.sql", {"stagingTable": self.__containStaging})
            space4StagePrdct = db.sql2TmpSql("space4StagePrdct.sql", {"stagingTable": self.__containStaging})
            impPdctFailTmpSql = db.sql2TmpSql("impPrductFailed.sql", {})
        except Exception as e:
            ex = str(e)
        else:
            pass
        finally:
            return ex

    def __generateExecCommand(self):
        dbworkingDir = ContextInitiator().getProperDict()['dbWorkingFloder'] + '/'
        db = DBHelper()
        self.__getSmr = db.dbExecCommand("smr2CmrPrdct.sql", self.__logsPath, "smr2CmrPrdct.log")
        self.__makeSpace = db.dbExecCommand("space4StagePrdct.sql", self.__logsPath, "space4StagePrdct.log")
        self.__ins2Differ = db.dbExecCommand("ins2Differ.sql")  # ,self.__logsPath,"ins2Differ.log"
        # self.__backupSnyc =  db.dbExecCommand("backupSnyc.sql")#,self.__logsPath,"backupSnyc.log"
        self.__smrImpFail = db.dbExecCommand("smrImpFailed.sql", self.__logsPath, "smrImpFailed.log")
        self.__productsData = db.dbExecCommand("getProduct.sql", self.__logsPath, "getProduct.log")
        self.__impPrdctFaild = db.dbExecCommand("impPrductFailed.sql", self.__logsPath, "impPrductFailed.log")
        self.__impSourceCSV = dbHelper.dbExecCmd4Remote(dbworkingDir, "impSmr2CmrPrdct.sql")
        self.__importProduct = dbHelper.dbExecCmd4Remote(dbworkingDir, "impProduct.sql")

        self.__impSourceCSVLocal = db.dbExecCommand("impSmr2CmrPrdct.sql")  # ,logsPath,"impSmrFromCsv.log"
        self.__importProductLocal = db.dbExecCommand("impProduct.sql")  # ,logsPath,"impProduct.log"

    def createNodeAndAliaLocally(self):
        msg = ''
        msg = self.__createNodeAndAliaSource()
        if msg.strip():
            return "sourceDB:" + msg

        msg = self.__createNodeAndAliaAim()
        if msg.strip():
            return "ourDB:" + msg
        return msg

    def dbConnTesting(self):
        ex = ''
        try:
            subprocess.check_call(self.__conn2RemoteDb, shell=True)
            subprocess.check_call(self.__conn2OurDb2, shell=True)
            sshc = SSHClient()
            sshc.SSHconnector()
            self.__sshCli = sshc.getSSH()
        except Exception as e:
            ex = str(e)
        else:
            pass
        finally:
            return ex

    def sshConnTesting(self):
        ex = ''
        try:
            sshc = SSHClient()
            sshc.SSHconnector()
            self.__sshCli = sshc.getSSH()
        except Exception as e:
            ex = str(e)
        else:
            pass
        finally:
            return ex

    def connDbAndSSH(self):
        errmsg = ''
        self.__DBConnStr()
        errmsg = self.__generateTmpSql()
        if errmsg.strip():
            return errmsg
        self.__generateExecCommand()
        # errmsg = self.createNodeAndAliaLocally()
        # if errmsg.strip():
        #     return errmsg
        exmsg = self.dbConnTesting()
        if exmsg.strip():
            return exmsg
        # exmsg = self.sshConnTesting()
        # if exmsg.strip():
        #     return exmsg
        return errmsg

    def makeSpace(self):
        errmsg = ''
        commandRes = execAndLog(self.__conn2OurDb2, self.__makeSpace, 'makeSpace4StagePrdct')
        if commandRes["returncode"] != 0:
            errmsg = 'makeSpace4StagePrdctFailed'
        return errmsg

    def getSmrProCsvs(self):
        errmsg = ''
        commandRes = execAndLog(self.__conn2RemoteDb, self.__getSmr, 'getSourceCSV')
        if commandRes["returncode"] != 0:
            errmsg = 'exportSourceCsvFailed'
            return errmsg

        commandRes = execAndLog(self.__conn2RemoteDb, self.__productsData, 'getProductCSV')
        if commandRes["returncode"] != 0:
            errmsg = 'exportProductCsvFailed'
            return errmsg
        return errmsg

    def transFilesViaSSH(self):
        dbWorkingFloder = self.__spDir4Outer.replace('\/', '/')
        ex = ''
        try:
            sshClient.transFileViaSSh(self.__sshCli, self.__csvFile, dbWorkingFloder, 'put')
            sshClient.transFileViaSSh(self.__sshCli, self.__sqlScript, dbWorkingFloder, 'put')
            sshClient.transFileViaSSh(self.__sshCli, self.__productCsvFile, dbWorkingFloder, 'put')
            sshClient.transFileViaSSh(self.__sshCli, self.__productSqlScript, dbWorkingFloder, 'put')
        except Exception as e:
            ex = str(e)
        else:
            pass
        finally:
            return ex

    def getFilesViaSSH(self):
        dbWorkingFloder = self.__spDir4Outer.replace('\/', '/')
        ex = ''
        try:
            sshClient.transFileViaSSh(self.__sshCli, dbWorkingFloder + "/impProductCsv_db2.log", self.__logsPath, 'get')
            sshClient.transFileViaSSh(self.__sshCli, dbWorkingFloder + "/impSmr2CmrPrdct_db2.log", self.__logsPath,
                                      'get')
        except Exception as e:
            ex = str(e)
        else:
            pass
        finally:
            return ex

    def importCSVs(self):
        errmsg = ''
        stdin, stdout, stderr = execRemoteCommand(self.__sshCli,
                                                  self.__sourceAixEnv + ' && ' + self.__connOuterSpaceDb + " && " + self.__impSourceCSV,
                                                  "impSmr2Cmr")
        normlMsg = stdout.readlines()
        errorMsg = stderr.readlines()
        # for x in xrange(1,len(normlMsg)):
        #	 print x,'::', normlMsg[x]
        if len(errorMsg) != 0 or (impDataMonitor(normlMsg) == False):
            execAndLog(self.__conn2OurDb2, self.__smrImpFail, 'handleImpSMRFailed')
            errmsg = 'sourceCsvImportFailed'
            return errmsg

        stdin, stdout, stderr = execRemoteCommand(self.__sshCli,
                                                  self.__sourceAixEnv + ' && ' + self.__connOuterSpaceDb + " && " + self.__importProduct,
                                                  "impPrdct")
        normlMsg = stdout.readlines()
        errorMsg = stderr.readlines()
        if len(errorMsg) != 0 or (impDataMonitor(normlMsg) == False):
            execAndLog(self.__conn2OurDb2, self.__impPrdctFaild, 'handleProductImpFailed')
            errmsg = 'ProductImportFailed'
            return errmsg
        return errmsg

    def importCSVLocally(self):
        errmsg = ''
        commandRes = execAndLog(self.__conn2OurDb2, self.__impSourceCSVLocal, 'impSmr2Cmr')
        if commandRes['returncode'] != 0 or (impDataMonitor(commandRes['back']) == False):
            execAndLog(self.__conn2OurDb2, self.__smrImpFail, 'handleImpSMRFailed')
            errmsg = 'impSourceCSVError'
            return errmsg

        commandRes = execAndLog(self.__conn2OurDb2, self.__importProductLocal, 'impPrdct')
        if commandRes['returncode'] != 0 or (impDataMonitor(commandRes['back']) == False):
            execAndLog(self.__conn2OurDb2, self.__impPrdctFaild, 'handleProductImpFailed')
            errmsg = 'importProductError'
            return errmsg
        return errmsg

    def toDiffTable(self):
        errmsg = ''
        commandRes = execAndLog(self.__conn2OurDb2, self.__ins2Differ, 'toDiff')
        if commandRes["returncode"] != 0 and isErrLegal(commandRes["back"]) == False:
            errmsg = 'ins2DifferError'
        return errmsg

    # def backupAndSnyc(self):
    # 	errmsg = ''
    # 	commandRes = execAndLog(self.__conn2OurDb2,self.__backupSnyc,'backupSnyc')
    # 	if commandRes["returncode"] != 0 and isErrLegal(commandRes["back"]) == False:
    # 		errmsg = 'backupSnycFiled'
    # 	return errmsg

    def __createNodeAndAliaSource(self):
        # create remote db's node and alia in local
        msg = ''
        commandRes = subprocess.call(self.__isNodeExist, shell=True)
        if commandRes != 0:
            commandRes = subprocess.call(self.__remoteDBNode, shell=True)
            if commandRes != 0:
                msg = 'createSourceDbNodeFailed'
                return msg

        commandRes = subprocess.call(self.__isAliaExist, shell=True)
        if commandRes != 0:
            commandRes = subprocess.call(self.__remoteDbAlia, shell=True)
            if commandRes != 0:
                msg = 'createSourceDbAliaFailed'
                return msg
        return msg

    def __createNodeAndAliaAim(self):
        # create remote db's node and alia in local
        msg = ''
        commandRes = subprocess.call(self.__isOurNodeExist, shell=True)
        if commandRes != 0:
            commandRes = subprocess.call(self.__ourDBNode, shell=True)
            if commandRes != 0:
                msg = 'createAimDbNodeFailed'

        commandRes = subprocess.call(self.__isOurAliaExist, shell=True)
        if commandRes != 0:
            commandRes = subprocess.call(self.__ourDbAlia, shell=True)
            if commandRes != 0:
                msg = 'createAimDbAliaFailed'
        return msg

    def uncataNodeAlia(self, needUncataDbNode, needUncataDbAlia):
        if needUncataDbNode:
            subprocess.call(self.__uncataNode, shell=True)
            subprocess.call(self.__uncataOurNode, shell=True)

        if needUncataDbAlia:
            subprocess.call(self.__uncataDBAlia, shell=True)
            subprocess.call(self.__uncataOurDBAlia, shell=True)


def execAndLog(dbconn, cmd, act):
    print act, ' startAt:', time.asctime(time.localtime(time.time()))
    startTime = time.time()
    env = '. /etc/profile && . /home/db2inst1/.profile'
    subThread = subprocess.Popen(env+' && '+dbconn + ' && ' + cmd, shell=True, stdout=subprocess.PIPE,env={"shell": "/usr/bin/bash"})
    feed = subThread.communicate()
    # subThread.wait() #may cause deadlock
    print act, ' finishAt:', time.asctime(time.localtime(time.time()))

    endTime = time.time()
    costTime = endTime - startTime
    sys.stdout.write("%s costTime: %.2f seconds" % (act, costTime))

    return {"returncode": subThread.returncode, "back": feed[0].split('\n')}


def execRemoteCommand(ssh, command, action):
    print action, ' startAt:', time.asctime(time.localtime(time.time()))
    startTime = time.time()

    stdin, stdout, stderr = ssh.exec_command(command)

    print action, ' finishAt:', time.asctime(time.localtime(time.time()))
    endTime = time.time()
    costTime = endTime - startTime
    sys.stdout.write("%s costTime: %.2f seconds" % (action, costTime))
    return stdin, stdout, stderr


def isErrLegal(msgTuple):
    '''
		if the shell running result is not equal to 0 but the error 
		msg include code 02000 then we consider the sql ran successfully
		because there's no data in select statement
	'''
    for ele in msgTuple:
        if -1 != ele.find("SQLSTATE=02000"):
            return True
    return False


def impDataMonitor(normlMsg):
    msgList = []
    dataMsg = []
    # Number of rows read ,rejected, committed respectively
    for ele in xrange(0, len(normlMsg)):
        if normlMsg[ele] is not None:
            if normlMsg[ele].find("Number of rows read") != -1:
                msgList.append(normlMsg[ele])
            elif normlMsg[ele].find("Number of rows committed") != -1:
                msgList.append(normlMsg[ele])
            elif normlMsg[ele].find("Number of rows rejected") != -1:
                msgList.append(normlMsg[ele])
    for ele in msgList:
        halfBrick = ele.split('=', 1)
        dataMsg.append(int(halfBrick[1].strip()))
    if len(dataMsg) >= 3:
        if dataMsg[1] == 0 and (dataMsg[0] == dataMsg[2]):
            return True
        else:
            return False
    else:
        return False
