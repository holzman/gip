"""
Common functions for the GIP validator.

This module should generally follow PEP 8 coding guidelines.
"""
import os
import sys
import time
import socket
import urllib
import tempfile

from gip_ldap import read_bdii
#pylint: disable-msg=W0105

# This evaluates to true if Python 2.3 or higher is available.
py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3


def getHostname():
    """
    Convenience function for retrieving the local hostname.
    """
    return socket.gethostbyaddr(socket.gethostname())

def read_file(path):
    """
    Return the contents of the file on a given path
    
    @param path: Path to file for reading
    @raise IOError: When file can't be read (doesn't exist, permission errors)
    @return: File contents.
    """
    f = open(path)
    return f.read()

def write_file(path, contents, append=False):
    """
    Writes some contents to a given path.  If append is False, the file is
    overwritten.
    
    @param path: Path to file we will append.
    @param contents: Additional contents for file.
    @param append: Append to file?. Default is False
    @raise IOError: When file can't be appended.
    """
    if append: flag = 'a' 
    else: flag = 'w'
    
    f = open(path, flag)
    f.write(contents)
    f.close()

def getURLData(some_url, lines=False):
    data = None
    filehandle = urllib.urlopen(some_url)
    if lines:
        data = filehandle.readlines()
    else:
        data = filehandle.read()

    return data

def getUrlFd(some_url):
    return urllib.urlopen(some_url)

def compare_by(fieldname):
    def compare_two_dicts (a, b):
        return cmp(a[fieldname], b[fieldname])
    return compare_two_dicts

def getTempFilename():
    try:
        conffile = tempfile.NamedTemporaryFile()
        conffile = conffile.name
    except:
        conffile = tempfile.mktemp()
    return conffile

def strContains(main_str, sub_str):
    result = False
    if py23:
        result = sub_str in main_str
    else:
        contains = lambda haystack, needle: haystack.find(needle) > -1
        if contains(main_str, sub_str) > 0: result = True
    
    return result

def runCommand(cmd):
    return os.popen(cmd)

def getBDIIParts(endpoint):
    parts = endpoint.split(":")
    return {"bdii": parts[1], "port": parts[2]}
    
def runlcginfo(opt, endpoint="ldap://is.grid.iu.edu:2170", VO="ops"):
    bdii_parts = getBDIIParts(endpoint)
    cmd = "lcg-info " + opt + " --vo " + VO + " --bdii " + bdii_parts["bdii"] + ":" + bdii_parts["port"]
    return runCommand(cmd)

def runlcginfosites(endpoint="ldap://is.grid.iu.edu:2170", VO="ops", opts=""):
    bdii_parts = getBDIIParts(endpoint)
    cmd = "lcg-infosites --is %s --vo %s %s" % (bdii_parts["bdii"], VO, opts)  
    return runCommand(cmd)

def message(msg_type, msg_str):
    msg_type = str(msg_type)
    msg_str = str(msg_str)
    return {"type": msg_type, "msg": msg_str}

MSG_INFO = "INFO"
MSG_CRITICAL = "CRIT"

def passed(msg_list):
    for msg in msg_list:
        if msg["type"] == MSG_CRITICAL: return False
    return True 

def getTimestamp(format="%a %b %d %T UTC %Y"):
    return time.strftime(format, time.gmtime())

def toBoolean(val):
    val = val.lower()
    if val.find('t') >= 0 or val.find('y') >= 0 or val.find('1') >= 0:
        return True
    if val.find('f') >= 0 or val.find('n') >= 0 or val.find('0') >= 0:
        return False

def ls(directory):
    return os.listdir(directory)

def getFQDNsBySiteName(cp, sitename):
    fqdn = []
    entries = read_bdii(cp, query="(objectClass=GlueCE)", base="mds-vo-name=%s,mds-vo-name=local,o=grid" % sitename)
    for entry in entries:
        if 'GlueCE' in entry.objectClass:
            fqdn.append(entry.glue['CEHostingCluster'])
    return fqdn

def addToPath(new_element):
    os.environ["PATH"] = "%s:%s" % (str(new_element), os.environ.get("PATH"))
