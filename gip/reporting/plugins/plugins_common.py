#!/usr/bin/env python

import os
import sys

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/reporting/plugins"))
from gip_common import config, cp_get, ls

class PluginError:
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)
class ConfigurationError(PluginError):
    pass
class EmptyNodeListError(PluginError):
    pass
class GenericXMLError(PluginError):
    pass

def search_list_by_name(name, list):
    for item in list:
        if item["Name"] == name:
            return item
    return None
        
def getTestResultFileList(results_dir, test_list):
    try:
        test_list = test_list.split(",")
        file_list = []
        ls_list = ls(results_dir)
        for item in ls_list:
            if item[:-4] in test_list:
                file_list.append(item)
        
    except Exception, e:
        file_list = None
        
    return file_list

def add_pluginlog_handler():
    """
    Add a log file to the default root logger.
    
    Uses a rotating logfile of 10MB, with 5 backups.
    """
    mylog = logging.getLogger()
    try:
        os.makedirs(os.path.expandvars('$GIP_LOCATION/var/logs'))
    except OSError, oe:
        #errno 17 = File Exists
        if oe.errno != 17:
            return
    logfile = os.path.expandvars('$GIP_LOCATION/var/logs/reporting_plugin.log')
    formatter = logging.Formatter('%(asctime)s %(name)s:%(levelname)s ' \
        '%(pathname)s:%(lineno)d:  %(message)s')
    handler = logging.handlers.RotatingFileHandler(logfile,
        maxBytes=1024*1024*10, backupCount=5)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    mylog.addHandler(handler)