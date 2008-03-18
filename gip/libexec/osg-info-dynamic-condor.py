#!/usr/bin/python

import re, sys, os
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate, voList,  printTemplate, cp_get
from condor_common import parseNodes, getJobsInfo, getLrmsInfo, getGroupInfo


log = getLogger("GIP.Condor")

def usage():
   print "Usage: osg-info-dynamic-condor.py <condor path> <ldif file> [central manager]\n"

def print_CE(cp):
    CE_plugin = getTemplate("GlueCEPlugin", "GlueCEUniqueID")
    ce_name = cp_get(cp, "ce", "name", "")
    status = cp_get(cp, "condor", "status", "Production")
    condorVersion = getLrmsInfo(cp)
    total_nodes, claimed, unclaimed = parseNodes(cp)
    vo_map = VoMapper(cp)
    jobs_info = getJobsInfo(vo_map, cp)
    groupInfo = getGroupInfo(vo_map, cp)
    for vo in voList(cp):
        info = jobs_info.get(vo, {"running": 0, "idle": 0, "held": 0})
        info = {"ce_name"     : ce_name,
                "job_manager" : 'condor',
                "queue"       : vo,
                "version"     : condorVersion,
                "job_slots"   : int(total_nodes),
                "free_slots"  : int(unclaimed),
                # Held jobs are included as "waiting" since the definition is:
                #    Number of jobs that are in a state different than running
                "wait"        : info["idle"] + info["held"],
                "running"     : info["running"],
                "total"       : info["running"] + info["idle"] + info["held"],
                "priority"    : groupInfo["prio"],
                "max_running" : groupInfo["quota"],
                "max_wall"    : 0,
                "status"      : status,
                "vo"          : vo
                }
        printTemplate(CE_plugin, info)
    return total_nodes, claimed, unclaimed

def print_VOViewLocal(cp):
    VOView_plugin = getTemplate("GlueCEPlugin", "GlueVOViewLocalID")
    ce_name = cp_get(cp, "ce", "name", "")

    status = cp_get(cp, "condor", "status", "Production")
    condorVersion = getLrmsInfo(cp)
    total_nodes, claimed, unclaimed = parseNodes(cp)

    vo_map = VoMapper(cp)
    jobs_info = getJobsInfo(vo_map, cp)
    groupInfo = getGroupInfo(vo_map, cp)

    for vo in voList(cp):
        info = jobs_info.get(vo, {"running": 0, "idle": 0, "held": 0})
        info = {"vo"          : vo,
                "ce_name"     : ce_name,
                "job_manager" : 'condor',
                "queue"       : vo,
                "running"     : info["running"],
                # Held jobs are included as "waiting" since the definition is:
                #    Number of jobs that are in a state different than running
                "wait"        : info["idle"] + info["held"],
                "total"       : info["running"] + info["idle"] + info["held"],
                "free_slots"  : unclaimed,
                "job_slots"   : total_nodes
                }
        printTemplate(VOView_plugin, info)

def main():
    try:
        cp = config()
        addToPath(cp.get("condor", "condor_path"))
        vo_map = VoMapper(cp)
        condorVersion = getLrmsInfo(cp)
        total_nodes, claimed, unclaimed = print_CE(cp)
        print_VOViewLocal(cp)
    except Exception, e:
        sys.stdout = sys.stderr
        log.error(e)
        raise

if __name__ == '__main__':
   main()

