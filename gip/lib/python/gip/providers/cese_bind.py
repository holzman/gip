#!/usr/bin/python
    
import re
import sys  
import os
        
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getLogger, getTemplate, printTemplate
from gip_cese_bind import getCESEBindInfo

log = getLogger("GIP.CESEBind")

def print_CESEBind(cp):
    group_template = getTemplate("GlueCESEBind", "GlueCESEBindGroupCEUniqueID")
    se_template = getTemplate("GlueCESEBind", "GlueCESEBindSEUniqueID")
    for info in getCESEBindInfo(cp):
        printTemplate(group_template, info)
        printTemplate(se_template, info)

def main():
    try:
        cp = config()
        print_CESEBind(cp)
    except Exception, e:
        sys.stdout = sys.stderr
        log.error(e)
        raise

if __name__ == '__main__':
    main()

