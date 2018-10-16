#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import sys
# import paramiko
# from scp import SCPClient
from contextInitiator import ContextInitiator


class SSHClient(object):
    __host = ''
    __port = ''
    __user = ''
    __pwd = ''
    __keyFile = ''
    __ssh = None

    """docstring for sshClient"""

    def __init__(self):
        super(SSHClient, self).__init__()
        dic = ContextInitiator().getProperDict()
        self.__host = dic['ourDbIp']
        self.__port = dic['dbServerPort']
        self.__user = dic['dbServerUser']
        self.__pwd = dic['dbServerPwd']
        self.__keyFile = ContextInitiator().getWorkingPath() + "/../rsakey"

    def getSSH(self):
        return self.__ssh

    def SSHconnector(self):
        # ssh = paramiko.SSHClient()
        # # allow to connect to the host which are not on know_host list
        # ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # if not self.__keyFile.strip():
        #     ssh.connect(self.__host, self.__port, self.__user, self.__pwd)
        # else:
        #     privatekeyfile = os.path.expanduser(self.__keyFile)
        #     rsaKey = paramiko.RSAKey.from_private_key_file(privatekeyfile, password=self.__pwd)
        #     transport = paramiko.Transport(self.__host, self.__port)
        #     transport.connect(username=self.__user, pkey=rsaKey)
        #     ssh._transport = transport
        #     self.__ssh = ssh
        # ssh.connect(host,port,user,pkey=key)
        pass


def transFileViaSSh(ssh, sourceFile, aim, act):
    # scp1 = SCPClient(ssh.get_transport(), progress=progress)
    # if act == 'put':
    #     scp1.put(sourceFile, aim)
    # if act == 'get':
    #     scp1.get(sourceFile, aim)
    # scp1.close()
    pass


def progress(filename, size, sent):
    percent = float(sent) / float(size) * 100
    sys.stdout.write("%s\'s progress: %.2f%%   \r" % (filename, percent))
