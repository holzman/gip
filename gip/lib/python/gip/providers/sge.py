#!/usr/bin/python

import re
import sys
import os

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate, printTemplate, cp_get
from gip_cluster import getClusterID
from gip_sections import ce

from sge_common import getQueueInfo, getJobsInfo, getLrmsInfo, getVoQueues

log = getLogger("GIP.SGE")

def print_CE(cp):
    SGEVersion = getLrmsInfo(cp)
    queueInfo = getQueueInfo(cp)
    ce_name = cp.get(ce, "name")
    CE = getTemplate("GlueCE", "GlueCEUniqueID")
    try:
        excludeQueues = [i.strip() for i in cp.get("sge", "queue_exclude").split(',')]
    except:
        excludeQueues = []

    vo_queues = getVoQueues(cp)

    default_max_waiting = 999999
    for queue in queueInfo.items():
        if queue in excludeQueues:
            continue
        unique_id = '%s:2119/jobmanager-pbs-%s' % (ce_name, queue['name'])

        acbr = ''
        for vo, queue2 in vo_queues:
            if queue == queue2:
                acbr += 'GlueCEAccessControlBaseRule: VO:%s\n' % vo

        info = { \
            "ceUniqueID" : unique_id
            "ceName" : ce_name
            "ceImpl" : 'Globus'
            "ceImplVersion" : cp_get(cp, ce, 'globus_version', '4.0.6')
            "clusterUniqueID" : getClusterID(cp)
            "queue" : queue['name']
            "priority" : queue['priority']
            "lrmsType" : 'sge'
            "lrmsVersion" : SGEVersion
            "job_manager" : "sge"
            "job_slots" : queue["slots_total"]
            "free_slots" : queue["slots_free"]
            "ert" : 3600
            "wrt" : 3600
            "hostingCluster" : cp_get(cp, ce, 'hosting_cluster', ce_name)
            "hostName" : cp_get(cp, ce, 'host_name', ce_name)
            "contact_string" : unique_id
            "app_dir" : cp.get('osg_dirs', 'app')
            "data_dir" : cp.get('osg_dirs', 'data')
            "default_se" : cp.get('se', 'name')
            "max_running" : queue["slots_total"]
            "max_wall" : queue["max_wall"]
            "max_waiting" : default_max_waiting
            "max_slots" : 1
            "max_total" : default_max_waiting + queue["slots_total"]
            "assigned" : queue["slots_used"]
            "preemption" : cp_get(cp, 'sge', 'preemption', '0')
            "acbr" : acbr[:-1]
            "bdii: " cp.get('bdii', 'endpoint')
            "gramVersion" : '2.0'
            "port" : 2119
            "waiting" : queue['waiting']
        }
        printTemplate(ce_template, info)
    return queueInfo

def print_VOViewLocal(cp):
    ce_name = cp.get(ce, "name")
    vo_map = VoMapper(cp)
    queue_jobs = getJobsInfo(vo_map, cp)
    VOView = getTemplate("GlueCE", "GlueVOViewLocalID")
    for vo in vo_queues:
        ce_unique_id = '%s:2119/jobmanager-sge-%s' % (ce_name, queue)
        info = {
            'ceUniqueID'  : ce_unique_id,
            'voLocalID'   : vo['vo'],
            'acbr'        : 'VO:%s' % vo['vo'],
            'running'     : vo.get('running', 0),
            'waiting'     : vo.get('waiting', 0),
            #'free_slots'  : vo.get(queue, {}).get('free_slots', 0),
            'ert'         : 3600,
            'wrt'         : 3600,
            'default_se'  : cp.get("se", "name"),
            'app'         : cp.get("osg_dirs", "app"),
            'data'        : cp.get("osg_dirs", "data"),
        }
        info['total'] = info['waiting'] + info['running']
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

