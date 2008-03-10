#!/usr/bin/python

import re
import sys
import os

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate
from pbs_common import parseNodes, getQueueInfo, getJobsInfo, getLrmsInfo, \
    getVoQueues

log = getLogger("GIP.PBS")

def print_CE(cp):
    pbsVersion = getLrmsInfo(cp)
    queueInfo = getQueueInfo(cp)
    totalCpu, freeCpu, queueCpus = parseNodes(cp, pbsVersion)
    ce_name = cp.get("ce", "name")
    CE_plugin = getTemplate("GlueCE", "GlueCEUniqueID")
    try:
        excludeQueues = [i.strip() for i in cp.get("pbs", \
            "queue_exclude").split(',')]
    except:
        excludeQueues = []
    for queue, info in queueInfo.items():
        if queue in excludeQueues:
            continue
        info["version"] = pbsVersion
        info["job_manager"] = "pbs"
        if info["wait"] > 0:
            info["free_slots"] = 0
        else:
            if queue in queueCpus:
                info["free_slots"] = queueCpus[queue]
            else:
                info["free_slots"] = freeCpu
        info["queue"] = queue
        info["ce_name"] = ce_name
        if "job_slots" not in info:
            info["job_slots"] = totalCpu
        if "priority" not in info:
            info["priority"] = 0
        if "max_running" not in info:
            info["max_running"] = info["job_slots"]
        if "max_wall" not in info:
            info["max_wall"] = 0
        info["job_slots"] = min(totalCpu, info["job_slots"])
        print CE_plugin % info
    return queueInfo, totalCpu, freeCpu, queueCpus

def print_VOViewLocal(queue_info, cp):
    ce_name = cp.get("ce", "name")
    vo_map = VoMapper(cp)
    queue_jobs = getJobsInfo(vo_map, cp)
    VOView = getTemplate("GlueCE", "GlueVOViewLocalID")
    vo_queues = getVoQueues(cp)
    for vo, queue in vo_queues:
    for queue, vo_info in queue_jobs.items():
        vo_info = queue_jobs.get(queue, {})
        info2 = vo_info.get(vo, {})
        info = {
            'job_slots' : queue_info.get(queue, {}).get('job_slots', 0),
            'free_slots' : queue_info.get(queue, {}).get('free_slots', 0),
            'ce_name' : ce_name,
            'queue' : queue,
            'vo' : vo,
            'job_manager' : pbs,
            'running' : info2.get('running', 0),
            'max_running' : info2.get('max_running', 0),
            'priority' : 
        }
        printTemplate(VOView, info)

def main():
    try:
        cp = config()
        addToPath(cp.get("pbs", "pbs_path"))
        vo_map = VoMapper(cp)
        pbsVersion = getLrmsInfo(cp)
        queueInfo, totalCpu, freeCpu, queueCpus = print_CE(cp)
        print_VOViewLocal(queueInfo, cp)
    except Exception, e:
        sys.stdout = sys.stderr
        log.error(e)
        raise

if __name__ == '__main__':
    main()

