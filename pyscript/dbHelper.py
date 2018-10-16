#!/usr/bin/python
# -*- coding: UTF-8 -*-
import subprocess
from contextInitiator import ContextInitiator


class DBHelper(object):
    """docstring for IBMdbHelper"""
    __dbAlia = 'db2 catalog db %s as %s at node %s'
    __dbNode = 'db2 catalog tcpip node %s remote %s server %s remote_instance %s'
    __conn2Db = 'db2 connect to %s user %s using %s'
    __isNodeExist = 'db2 list node directory | grep -i %s'
    __isAliaExist = 'db2 list database directory | grep -i %s'
    __uncataDBAlia = 'db2 uncatalog db %s'
    __uncataNode = 'db2 uncatalog node %s'
    __connOuterSpaceDb = 'db2 connect to %s user %s using %s'
    __properDict = ContextInitiator().getProperDict()
    __sqlFloder = ContextInitiator().getWorkingPath() + "/../sqls/"

    def __init__(self):
        super(DBHelper, self).__init__()

    def getOuterSpaceDbCmd(self, replacements):
        return self.__connOuterSpaceDb % replacements

    def getDbAliaCmd(self, replacements):  # 定义set方法
        return self.__dbAlia % replacements

    def getDbNodeCmd(self, replacements):
        return self.__dbNode % replacements

    def getConn2DbCmd(self, replacements):
        return self.__conn2Db % replacements

    def isNodeExistCmd(self, replacements):
        return self.__isNodeExist % replacements

    def isAliaExistCmd(self, replacements):
        return self.__isAliaExist % replacements

    def getUncataDBAliaCmd(self, replacements):
        return self.__uncataDBAlia % replacements

    def getUncataNode(self, replacements):
        return self.__uncataNode % replacements

    def dbExecCommand(self, sqlFile, logsPath='', logName=''):
        sqlPath = self.__sqlFloder
        var = "db2 -tvf " + sqlPath + "tmp_" + sqlFile
        if logsPath.strip() and logName.strip():
            var = var + " > " + logsPath + logName
        return var

    def sql2TmpSql(self, sourceSql, sedDiect):
        sqlPath = self.__sqlFloder
        command = "cat " + sqlPath + sourceSql
        for ele in sedDiect:
            command = command + " | sed 's/${" + ele + "}/" + sedDiect[ele] + "/g'"
        command = command + "> " + sqlPath + "tmp_" + sourceSql
        subprocess.call(command, shell=True)


# def unCatalog(uncataNode,uncataOurNode):
# 	subprocess.call(uncataNode ,shell=True)
# 	subprocess.call(uncataOurNode ,shell=True)

def dbExecCmd4Remote(path, sqlFile, logsPath='', logName=''):
    var = "db2 -tvf " + path + "tmp_" + sqlFile
    if logsPath.strip() and logName.strip():
        var = var + " > " + logsPath + logName
    return var
