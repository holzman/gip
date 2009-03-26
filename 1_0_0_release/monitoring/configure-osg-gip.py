#!/usr/bin/env python

import sys
import os

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getOsgAttributes
from user_input import *

q = GipQuestions()

valid_batch = ["condor", "pbs", "lsf", "sge"]
def config_batch(ask):
    """
    Ask about the batch system.  Die if PBS/Condor environment isn't set
    up correctly.
    """
    batch = ask(q.batch, 'ce', 'job_manager', oneOfList, valid_batch)
    if batch == "condor":
        env_path = os.path.expandvars("$VDT_LOCATION/vdt/etc/condor-env.sh")
        if not os.path.exists(env_path):
            raise GipConfigError("Condor Environment Missing")
    elif batch == "pbs":
        try:
            which(qstat)
        except:
            raise GipConfigError("PBS Missing Qstat")
    elif batch == "lsf" or batch == "sge":
        pass

def config_sa_paths(ask):
    """
    Configure the storage area (SA) for this storage element.
    """
    print q.sa_intro
    ask(q.sa_root_path, 'sa', 'root_path')
    if ask(q.sa_has_exceptions, 'sa', 'has_exceptions'):
        vo = ask(q.sa_vo_exception, None, None)
        vo_path = ask(q.sa_exception_path % {'vo':vo}, 'sa', '%s')
        while ask(q.sa_more_exceptions, None, None):
            vo = ask(q.sa_vo_exception, None, None)
            vo_path = ask(q.sa_exception_path % {'vo':vo}, 'sa', '%s')

def config_standalone_gftp(ask):
    """
    Ask whether or not we should publish a standalone GridFTP server.
    """
    answer = ask(q.standalone_gftp, 'se', 'standalone_gftp', makeBoolean)
    if not answer:
        return

def config_se(ask):
    """
    Ask about the SE technology at the site.  
    """
    ask(q.se_name, 'se', 'name')
    ask(q.se_hostname, 'se', 'hostname')
    answer = ask(q.se_tech, 'se', 'system')
    if answer.lower() == 'dcache' and \
            ask(q.dcache_automate, 'se', 'dcache_automate', makeBoolean):
        pass
    else:
        pass

def config_ce(ask):
    """
    Configure the local Compute Element.
    """
    confirm_ce(ask)

def config_sc(ask):
    """
    Configure the SubClusters for this CE.
    """
    
def confirm_site(ask):
    """
    Confirm the site details as written by configure-osg.sh
    """
    info = getOsgAttributes()
    print q.confirm_site % info

def config_site(ask):
    """
    Configure the site.
    """
    confirm_site(ask)
    print ask(q.config_ce, 'site', 'has_ce', makeBoolean)
    if ask(q.config_ce, 'site', 'has_ce', makeBoolean):
        config_ce(ask)
    if ask(q.config_se, 'site', 'has_se', makeBoolean):
        config_se(ask)
   
def save(cp):
    """
    Save the information in the ConfigParser to gip.conf
    """
    bkp_num = os.path.expandvars("$GIP_LOCATION/etc/gip.conf.backup.%i")
    bkp = os.path.expandvars("$GIP_LOCATION/etc/gip.conf.backup")
    save_point = os.path.expandvars("$GIP_LOCATION/etc/gip.conf")
    backup_name = None
    if os.path.exists(save_point):
        backup_name = bkp
        if os.path.exists(bkp):
            counter = 1
            while os.path.exists(bkp_num % counter):
                counter += 1
            backup_name = bkp_num % counter
        os.rename(save_point, backup_name)
    cp.write(open(save_point, 'w'))
    print q.post_save

def main():
    cp = config()
    ask = InputHandler(cp)
    try:
        config_site(ask)
    except KeyboardInterrupt:
        if ask(q.save_progress, None, None, makeBoolean):
            save(cp)
        else:
            sys.exit(1)
    save(cp)

if __name__ == '__main__':
    main()

