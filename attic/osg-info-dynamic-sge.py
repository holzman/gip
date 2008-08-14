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
    ce_name = cp.get("ce", "name")
    CE_plugin = getTemplate("GlueCEPlugin", "GlueCEUniqueID")
    try:
        excludeQueues = [i.strip() for i in cp.get("sge", "queue_exclude").split(',')]
    except:
        excludeQueues = []

    for queue, info in queueInfo.items():
        if queue in excludeQueues:
            continue
        info["version"] = sgeVersion
        info["job_manager"] = "sge"
        info["ce_name"] = ce_name

        print CE_plugin % info
    return queueInfo

def print_VOViewLocal(queue_info, cp):
    ce_name = cp.get("ce", "name")
    VOView_plugin = getTemplate("GlueCEPlugin", "GlueVOViewLocalID")
    try:
        queue_exclude = [i.strip() for i in cp.get("sge", "queue_exclude").split(',')]
    except:
        queue_exclude = []

    for queue, info in queue_info.items():
        queue_jobs = info["job_data"]

        if jobs_info["queue"] in queue_exclude:
            continue

        try:
            whitelist = [i.strip() for i in cp.get("sge", "%s_whitelist" % queue).split(',')]
        except:
            whitelist = []

        try:
            blacklist = [i.strip() for i in cp.get("sge", "%s_blacklist" % queue).split(',')]
        except:
            blacklist = []

        for vo, info in queue_jobs.items():
            if (vo in blacklist or "*" in blacklist) and vo not in whitelist:
                continue
            info["job_slots"] = queue_info.get(queue, {}).get("job_slots", 0)
            info["free_slots"] = queue_info.get(queue, {}).get("free_slots", 0)
            info["ce_name"] = ce_name
            info["vo"] = vo
            info["job_manager"] = "sge"
            print VOView_plugin % info

def main():
    try:
        cp = config()
        addToPath(cp.get("sge", "sge_path"))
        queueInfo = print_CE(cp)
        print_VOViewLocal(queueInfo, cp)
    except Exception, e:
        log.error(e)
        raise

if __name__ == '__main__':
    main()
