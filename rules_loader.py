#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import subprocess
from pyscript.contextInitiator import ContextInitiator
from pyscript.dbOperator import DBOperator


# import csv
# import shlex
# import ibm_db

def sendEmail(errMsg):
    print "message: " + errMsg


def sendMessageAndExit(msg, *cleanTmpFile):
    sendEmail(msg)
    for cmd in cleanTmpFile:
        subprocess.call(cmd, shell=True)
    sys.exit()


if __name__ == "__main__":

    context = ContextInitiator()
    needUncataDbAlia = context.getProperDict()['needUncataDbAlia']
    needUncataDbNode = context.getProperDict()['needUncataDbNode']

    if needUncataDbAlia == '1':
        needUncataDbAlia = True
    elif needUncataDbAlia == '0':
        needUncataDbAlia = False

    if needUncataDbNode == '1':
        needUncataDbNode = True
    elif needUncataDbNode == '0':
        needUncataDbNode = False

    cleanTmp = "rm " + context.getWorkingPath() + "/../sqls/tmp_*"

    cleanTmp = ""

    # create temp sql files
    # batchId = str(uuid.uuid1())
    dbOp = DBOperator()

    msg = dbOp.connDbAndSSH()

    if not msg.strip():
        # makeSpace
        # commandRes = dbOp.makeSpace()
        # if commandRes.strip():
        #     sendMessageAndExit(commandRes, cleanTmp)

        commandRes = dbOp.getSmrProCsvs()
        if commandRes.strip():
            sendMessageAndExit(commandRes, cleanTmp)

        commandRes = dbOp.importCSVLocally()
        if commandRes.strip():
            sendMessageAndExit(commandRes, cleanTmp)

        # commandRes = dbOp.transFilesViaSSH()
        # if commandRes.strip():
        # 	sendMessageAndExit(commandRes,cleanTmp)

        # commandRes = dbOp.importCSVs()
        # if commandRes.strip():
        # 	sendMessageAndExit(commandRes,cleanTmp)

        # commandRes = dbOp.getFilesViaSSH()
        # if commandRes.strip():
        # 	sendMessageAndExit(commandRes,cleanTmp)

        # commandRes = dbOp.toDiffTable()
        # if commandRes.strip():
        #     sendMessageAndExit(commandRes, cleanTmp)

    commandRes = dbOp.backupAndSnyc()
    if commandRes.strip():
    	sendMessageAndExit(commandRes,cleanTmp)
    else:
        sendMessageAndExit('connectFaild: ' + msg, cleanTmp)

    # uncatlog remote db's node and alias
    dbOp.uncataNodeAlia(needUncataDbNode, needUncataDbAlia)
    # set Table Phase
    context.setTablePhases()
    # clean tmp files
    sendMessageAndExit('success', cleanTmp)
