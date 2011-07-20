#!/usr/bin/python
    
import re
import sys  
import os
        
if 'GIP_LOCATION' in os.environ:
    sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
    
from gip_common import config, getLogger, getTemplate, printTemplate
from gip_cese_bind import getCESEBindInfo
import gip_sets as sets

log = getLogger("GIP.CESEBind")

def print_CESEBind(cp):
    group_template = getTemplate("GlueCESEBind", "GlueCESEBindGroupCEUniqueID")
    se_template = getTemplate("GlueCESEBind", "GlueCESEBindSEUniqueID")
    bind_info = getCESEBindInfo(cp)
    cegroups = {}
    for info in bind_info:
        printTemplate(se_template, info)
        ses = cegroups.setdefault(info['ceUniqueID'], sets.Set())
        ses.add(info['seUniqueID'])
    for ce, ses in cegroups.items():
        ses = '\n'.join(['GlueCESEBindGroupSEUniqueID: %s' % i for i in ses])
        info = {'ceUniqueID': ce, 'se_groups': ses}
        printTemplate(group_template, info)

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

