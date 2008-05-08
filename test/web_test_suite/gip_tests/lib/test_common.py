#!/usr/bin/env python

import os
import sys
import popen2
import urllib
import ConfigParser

def ls(directory):
    return os.listdir(directory)

def compare_by (fieldname):
    def compare_two_dicts (a, b):
        return cmp(a[fieldname], b[fieldname])
    return compare_two_dicts

def runCommand(command):
    pout = os.popen(command)
    return pout

def fileOverWrite(path, contents):
    owFile = open(path,"w")
    owFile.write(contents)
    owFile.close()

def getURLData(some_url, lines=False):
    data = None
    filehandle = urllib.urlopen(some_url)
    if lines:
        data = filehandle.readlines()
    else:
        data = filehandle.read()

    return data

def getConfig(base_path):
    cp = ConfigParser.ConfigParser()
    cp.readfp(open(base_path + "/etc/tests.conf"))
    return cp

def runlcginfo(opt, bdii="is.grid.iu.edu", port="2170", VO="ops"):
    cmd = "lcg-info " + opt + " --vo " + VO + " --bdii " + bdii + ":" + port
    return runCommand(cmd)

def runlcginfosites(bdii="is.grid.iu.edu", VO="ops", opts_list=[]):
    cmd = "lcg-infosites --is " + bdii + " --vo " + VO + " "
    for opt in opts_list:
        cmd += opt + " "
    return runCommand(cmd)
