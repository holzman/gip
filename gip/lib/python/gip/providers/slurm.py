#!/usr/bin/python

import re
import sys
import os

if 'GIP_LOCATION' in os.environ:
    sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))

import gip_cluster
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate, \
    printTemplate, cp_get, responseTimes, cp_getBoolean
from gip_cluster import getClusterID
from slurm_common import parseNodes, getQueueInfo, getJobsInfo, getLrmsInfo, \
    getVoQueues
from gip_sections import ce
from gip_storage import getDefaultSE
from gip_batch import buildCEUniqueID, getGramVersion, getCEImpl, getPort, \
     buildContactString, getHTPCInfo

log = getLogger("GIP.SLURM")

def print_CE(cp):
    slurmVersion = getLrmsInfo(cp)
    queueInfo = getQueueInfo(cp)
    ce_name = cp_get(cp, ce, "name", "UNKNOWN_CE")
    CE = getTemplate("GlueCE", "GlueCEUniqueID")
    try:
        excludeQueues = [i.strip() for i in cp_get(cp, "slurm", \
            "queue_exclude", "").split(',')]
    except:
        excludeQueues = []
    vo_queues = getVoQueues(cp)
    for queue, info in queueInfo.items():
        if queue in excludeQueues:
            continue
        info["lrmsVersion"] = slurmVersion
        info["job_manager"] = "slurm"

        # if no jobs are waiting in the queue, set the number of free slots
        # to (job_slots - running), or the total number of free slots on the cluster,
        # whichever is less.

        info["queue"] = queue
        info["ceName"] = ce_name

        unique_id = buildCEUniqueID(cp, ce_name, 'slurm', queue)
        ceImpl, ceImplVersion = getCEImpl(cp)
	port = getPort(cp)

        info['ceUniqueID'] = unique_id
        if "job_slots" not in info:
            log.error("no job_slots found for %s!" % queue)
        if "priority" not in info:
            info["priority"] = 0
        if "max_running" not in info:
            log.error("no max_running found for %s!" % queue)
        if "max_wall" not in info:
            info["max_wall"] = 1440

        
        info["free_slots"] = 0
        if info["wait"] == 0:
            freeSlots = info["job_slots"] - info["running"]
            if freeSlots > 0:
                info["free_slots"] =  freeSlots

        ert, wrt = responseTimes(cp, info.get("running", 0),
            info.get("wait", 0), max_job_time=info["max_wall"])

        info['ert'] = ert
        info['wrt'] = wrt
        info['hostingCluster'] = cp_get(cp, ce, 'hosting_cluster', ce_name)
        info['hostName'] = cp_get(cp, ce, 'host_name', ce_name)
        info['ceImpl'] = ceImpl
        info['ceImplVersion'] = ceImplVersion

	contact_string = buildContactString(cp, 'slurm', queue, unique_id, log)

        info['contact_string'] = contact_string
        info['app_dir'] = cp_get(cp, 'osg_dirs', 'app', "/UNKNOWN_APP")
        info['data_dir'] = cp_get(cp, 'osg_dirs', 'data', "/UNKNOWN_DATA")
        info['default_se'] = getDefaultSE(cp)

        if 'max_waiting' not in info:
            info['max_waiting'] = 999999
        if 'max_queuable' in info:
            info['max_total'] = info['max_queuable']
            info['free_slots'] = min(info['free_slots'], info['max_queuable'])
        else:
            info['max_total'] = info['max_waiting'] + info['max_running']
            info['free_slots'] = min(info['free_slots'], info['max_total'])

        # Enforce invariants:
        # max_total <= max_running
        # free_slots <= max_running
        info['max_total'] = min(info['max_total'], info['max_running'])
        info['free_slots'] = min(info['free_slots'], info['max_running'])

        info['assigned'] = info['job_slots']
        # Enforce invariants:
        # assigned <= max_running
        info['assigned'] = min(info['assigned'], info['max_running'])

        info['lrmsType'] = 'slurm'
        info['preemption'] = cp_get(cp, 'slurm', 'preemption', '0')
        acbr = ''
        has_vo = False
        for vo, queue2 in vo_queues:
            if queue == queue2:
                acbr += 'GlueCEAccessControlBaseRule: VO:%s\n' % vo
                has_vo = True
        if not has_vo:
            continue
        info['acbr'] = acbr[:-1]
        info['bdii'] = cp.get('bdii', 'endpoint')
        gramVersion = getGramVersion(cp)

        info['gramVersion'] = gramVersion
        info['port'] = port
        info['waiting'] = info['wait']
        info['referenceSI00'] = gip_cluster.getReferenceSI00(cp)
        info['clusterUniqueID'] = getClusterID(cp)

        extraCapabilities = ''
        if cp_getBoolean(cp, 'site', 'glexec_enabled', False):
            extraCapabilities = extraCapabilities + '\n' + 'GlueCECapability: glexec'

        htpcRSL, maxSlots = getHTPCInfo(cp, 'slurm', queue, log)
        info['max_slots'] = maxSlots
        
        if maxSlots > 1:
            extraCapabilities = extraCapabilities + '\n' + 'GlueCECapability: htpc'

        info['extraCapabilities'] = extraCapabilities
        info['htpc'] = htpcRSL

        print CE % info
    return queueInfo

def print_VOViewLocal(queue_info, cp):
    ce_name = cp_get(cp, ce, "name", "UNKNOWN_CE")
    vo_map = VoMapper(cp)
    queue_jobs = getJobsInfo(vo_map, cp)
    VOView = getTemplate("GlueCE", "GlueVOViewLocalID")
    vo_queues = getVoQueues(cp)
    for vo, queue in vo_queues:
        vo_info = queue_jobs.get(queue, {})
        info2 = vo_info.get(vo, {})

	port = getPort(cp)
        ce_unique_id = buildCEUniqueID(cp, ce_name, 'slurm', queue)
        
        my_queue_info = queue_info.setdefault(queue, {})
        ert, wrt = responseTimes(cp, info2.get("running", 0),
            info2.get("wait", 0),
            max_job_time=my_queue_info.get("max_wall", 0))

        free_slots = my_queue_info.get('free_slots', 0)
        waiting = info2.get('wait', 0)
        if waiting > 0:
            free_slots = 0

        info = {
            'ceUniqueID'  : ce_unique_id,
            'job_slots'   : my_queue_info.get('job_slots', 0),
            'free_slots'  : free_slots,
            'ce_name'     : ce_name,
            'queue'       : queue,
            'vo'          : vo,
            'voLocalID'   : vo,
            'job_manager' : 'slurm',
            'running'     : info2.get('running', 0),
            'max_running' : info2.get('max_running', 0),
            'priority'    : queue_info.get(queue, {}).get('priority', 0),
            'waiting'     : waiting,
            'data'        : cp_get(cp, "osg_dirs", "data", "UNKNOWN_DATA"),
            'app'         : cp_get(cp, "osg_dirs", "app", "UNKNOWN_APP"),
            'default_se'  : getDefaultSE(cp),
            'ert'         : 3600,
            'wrt'         : 3600,
            'acbr'        : 'VO:%s' % vo
        }
        info['total'] = info['waiting'] + info['running']
        printTemplate(VOView, info)

def main():
    try:
        cp = config()
        slurm_path = cp_get(cp, "slurm", "slurm_path", ".")
        addToPath(slurm_path)
        # adding slurm_path/bin to the path as well, since slurm/torque home
        # points to /usr/local and the binaries exist in /usr/local/bin
        addToPath(slurm_path + "/bin")
        vo_map = VoMapper(cp)
        slurmVersion = getLrmsInfo(cp)
        queueInfo = print_CE(cp)
        print_VOViewLocal(queueInfo, cp)
    except Exception, e:
        sys.stdout = sys.stderr
        log.exception(e)
        raise

if __name__ == '__main__':
    main()

