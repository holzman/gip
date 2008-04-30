#!/usr/bin/python
"""
Sun Grid Engine information provider.
"""

import re
import sys
import os
import time

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate
from sge_common import parseNodes, getQueueInfo, getJobsInfo, getLrmsInfo

def now():
    """
        returns a timestamp in GMT format
    """
    return = time.strftime("%a %b %d %T UTC %Y", time.gmtime())

def print_CE(cp):
    sgeVersion = getLrmsInfo(cp)
    queueInfo = getQueueInfo(cp)
    totalCpu, freeCpu, queueCpus = parseNodes(cp, sgeVersion)
    ce_name = cp.get("ce", "name")
    CE_plugin = getTemplate("GlueCEPlugin", "GlueCEUniqueID")
    try:
        excludeQueues = [i.strip() for i in cp.get("sqe", "queue_exclude").split(',')]
    except:
        excludeQueues = []
    for queue, info in queueInfo.items():
        if queue in excludeQueues:
            continue
        info["version"] = sgeVersion
        info["job_manager"] = "sge"
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
    VOView_plugin = getTemplate("GlueCEPlugin", "GlueVOViewLocalID")
    try:
        queue_exclude = [i.strip() for i in cp.get("sge", "queue_exclude").split(',')]
    except:
        queue_exclude = []
    for queue, vo_info in queue_jobs.items():
        if queue in queue_exclude:
            continue
        try:
            whitelist = [i.strip() for i in cp.get("sge", "%s_whitelist" % queue).split(',')]
        except:
            whitelist = []
        try:
            blacklist = [i.strip() for i in cp.get("sge", "%s_blacklist" % queue).split(',')]
        except:
            blacklist = []
        for vo, info in vo_info.items():
            if (vo in blacklist or "*" in blacklist) and vo not in whitelist:
                continue
            info["job_slots"] = queue_info.get(queue, {}).get("job_slots", 0)
            info["free_slots"] = queue_info.get(queue, {}).get("free_slots", 0)
            info["ce_name"] = ce_name
            info["queue"] = queue
            info["vo"] = vo
            info["job_manager"] = "sge"
            print VOView_plugin % info

def main():
    try:
        cp = config()
        addToPath(cp.get("sge", "sge_path"))
        queueInfo, totalCpu, freeCpu, queueCpus = print_CE(cp)
        print_VOViewLocal(queueInfo, cp)
    except Exception, e:
        log.error(e)
        raise

if __name__ == '__main__':
    main()
