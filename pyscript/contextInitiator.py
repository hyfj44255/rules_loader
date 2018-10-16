#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import property
import threading


class ContextInitiator(object):
    """docstring for ContextInitiator"""
    __workingPath = ''
    __properKeys = (
    'nodeName', 'dbIp', 'port', 'alia', 'usr', 'pwd', 'dbInstance', 'ourNodeName', 'ourDbIp', 'ourPort', 'ourAlia',
    'ourUsr', 'ourPwd', 'ourDbInstance', 'tablesPhases', 'dbWorkingFloder', 'tablesPhases', 'dbServerPort',
    'dbServerUser', 'dbServerPwd', 'needUncataDbAlia', 'needUncataDbNode')
    __properFile = '/../dbDriver.properties'
    __properDict = {}
    __containStaging = ''
    __containData = ''
    __containSS = ''
    __instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        # the core of singleton thread safe
        if not hasattr(ContextInitiator, "_instance"):
            with ContextInitiator.__instance_lock:
                if not hasattr(ContextInitiator, "_instance"):
                    ContextInitiator._instance = object.__new__(cls)
                    ContextInitiator._instance.__setWorkingPath()
                    ContextInitiator._instance.__setEnvs()
                    ContextInitiator._instance.__setTableSettings()
        return ContextInitiator._instance

    def __init__(self):
        super(ContextInitiator, self).__init__()

    def __setWorkingPath(self):
        if not self.__workingPath.strip():
            filePath = __file__
            fileName = os.path.basename(__file__)
            self.__workingPath = filePath.replace("/" + fileName, "")

    def __setEnvs(self):
        # get setting from sys environment
        envDist = os.environ
        for key in self.__properKeys:
            self.__properDict[key] = envDist.get(key)
        # get setting from property file if it's null in sys environment
        propertyFile = self.__workingPath + self.__properFile
        props = property.parse(propertyFile)
        for key in self.__properKeys:
            if self.__properDict[key] is None or not self.__properDict[key].strip():
                self.__properDict[key] = props.get(key)

    def __setTableSettings(self):
        if self.__properDict['tablesPhases'] == '0':
            self.__containStaging = 'RULES_DATA_STAGING'
            self.__containData = 'RULES_DATA'
            self.__containSS = 'RULES_DATA_SS'
        elif self.__properDict['tablesPhases'] == '1':
            self.__containStaging = 'RULES_DATA_SS'
            self.__containData = 'RULES_DATA_STAGING'
            self.__containSS = 'RULES_DATA'
        elif self.__properDict['tablesPhases'] == '2':
            self.__containStaging = 'RULES_DATA'
            self.__containData = 'RULES_DATA_SS'
            self.__containSS = 'RULES_DATA_STAGING'
        else:
            pass

    def setTablePhases(self):
        propertyFile = self.__workingPath + self.__properFile
        props = property.parse(propertyFile)
        if self.__properDict['tablesPhases'] == '0':
            props.put('tablesPhases', '1')

        if self.__properDict['tablesPhases'] == '1':
            props.put('tablesPhases', '2')

        if self.__properDict['tablesPhases'] == '2':
            props.put('tablesPhases', '0')

    def getContainStaging(self):
        return self.__containStaging

    def getContainData(self):
        return self.__containData

    def getContainSS(self):
        return self.__containSS

    def getWorkingPath(self):
        return self.__workingPath

    def getProperDict(self):
        return self.__properDict
