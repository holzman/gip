#!/usr/bin/python

import re
import sys
import os

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
import gip_cluster
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate, printTemplate, cp_get
from gip_cluster import getClusterID
from gip_sections import ce

from sge_common import getQueueInfo, getJobsInfo, getLrmsInfo, getVoQueues, \
    getQueueList

log = getLogger("GIP.SGE")

def print_CE(cp):
    SGEVersion = getLrmsInfo(cp)
    queueInfo, _ = getQueueInfo(cp)
    ce_name = cp_get(cp, ce, "name", "UNKNOWN_CE")
    ce_template = getTemplate("GlueCE", "GlueCEUniqueID")
    queueList = getQueueList(cp)

    vo_queues = getVoQueues(cp)

    default_max_waiting = 999999
    for queue in queueInfo.values():
        if 'name' not in queue or queue['name'] not in queueList:
            continue
        if queue['name'] == 'waiting':
            continue

        unique_id = '%s:2119/jobmanager-sge-%s' % (ce_name, queue['name'])

        acbr = ''
        for vo, queue2 in vo_queues:
            if queue['name'] == queue2:
                acbr += 'GlueCEAccessControlBaseRule: VO:%s\n' % vo

        referenceSI00 = gip_cluster.getReferenceSI00(cp)

        info = { \
            "ceUniqueID" : unique_id,
            "ceName" : ce_name,
            "ceImpl" : 'Globus',
            "ceImplVersion" : cp_get(cp, ce, 'globus_version', '4.0.6'),
            "clusterUniqueID" : getClusterID(cp),
            "queue" : queue['name'],
            "priority" : queue['priority'],
            "lrmsType" : 'sge',
            "lrmsVersion" : SGEVersion,
            "job_manager" : "sge",
            "job_slots" : queue["slots_total"],
            "free_slots" : queue["slots_free"],
            "running" : queue["slots_used"],
            "status" : queue['status'],
            "total" : queue['slots_used'] + queue['waiting'],
            "ert" : 3600,
            "wrt" : 3600,
            "hostingCluster" : cp_get(cp, ce, 'hosting_cluster', ce_name),
            "hostName" : cp_get(cp, ce, 'host_name', ce_name),
            "contact_string" : unique_id,
            "app_dir" : cp_get(cp, 'osg_dirs', 'app', "/OSG_APP_UNKNOWN"),
            "data_dir" : cp_get(cp, 'osg_dirs', 'data', "/OSG_DATA_UNKNOWN"),
            "default_se" : cp_get(cp, 'se', 'name', "DEFAULT_SE"),
            "max_running" : queue["slots_total"],
            "max_wall" : queue["max_wall"],
            "max_waiting" : default_max_waiting,
            "max_slots" : 1,
            "max_total" : default_max_waiting + queue["slots_total"],
            "assigned" : queue["slots_used"],
            "preemption" : cp_get(cp, 'sge', 'preemption', '0'),
            "acbr" : acbr[:-1],
            "bdii": cp.get('bdii', 'endpoint'),
            "gramVersion" : '2.0',
            "port" : 2119,
            "waiting" : queue['waiting'],
            "referenceSI00": referenceSI00,
        }
        printTemplate(ce_template, info)
    return queueInfo

def print_VOViewLocal(cp):
    ce_name = cp_get(cp, ce, "name", "UNKNOWN_CE")
    vo_map = VoMapper(cp)
    queue_jobs = getJobsInfo(vo_map, cp)
    vo_queues = getVoQueues(cp)
    VOView = getTemplate("GlueCE", "GlueVOViewLocalID")
    for vo, queue in vo_queues:
        ce_unique_id = '%s:2119/jobmanager-sge-%s' % (ce_name, queue)
        info = {
            'ceUniqueID'  : ce_unique_id,
            'voLocalID'   : vo,
            'acbr'        : 'VO:%s' % vo,
            'running'     : queue_jobs.get(queue, {}).get('running', 0),
            'waiting'     : queue_jobs.get(queue, {}).get('waiting', 0),
            #'free_slots'  : vo.get(queue, {}).get('free_slots', 0),
            'free_slots'  : 0, #TODO: fix
            'ert'         : 3600,
            'wrt'         : 3600,
            'default_se'  : cp_get(cp, "se", "name", "DEFAULT_SE"),
            'app'         : cp_get(cp, "osg_dirs", "app", "/OSG_APP_UNKNOWN"),
            'data'        : cp_get(cp, "osg_dirs", "data", "/OSG_DATA_UNKNOWN"),
        }
        info['total'] = info['waiting'] + info['running']
        printTemplate(VOView, info)

def main():
    try:
        cp = config()
        addToPath(cp_get(cp, "sge", "sge_path", "."))
        vo_map = VoMapper(cp)
        pbsVersion = getLrmsInfo(cp)
        print_CE(cp)
        print_VOViewLocal(cp)
    except Exception, e:
        sys.stdout = sys.stderr
        log.error(e)
        raise

if __name__ == '__main__':
    main()

