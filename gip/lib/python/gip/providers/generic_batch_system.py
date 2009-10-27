#!/usr/bin/python

import re
import sys
import os

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
import gip_cluster
from gip_common import config, getLogger, getTemplate, \
    printTemplate, cp_get, responseTimes
from gip_cluster import getClusterID, getClusterName
from gip_sections import ce
from gip_storage import getDefaultSE

from gip.batch_systems.pbs import PbsBatchSystem
from gip.batch_systems.condor import CondorBatchSystem
from gip.batch_systems.forwarding import Forwarding
from gip.batch_systems.sge import SgeBatchSystem
from gip.batch_systems.lsf import LsfBatchSystem

log = getLogger("GIP.Batch")

def print_CE(batch):
    system_name, version = batch.getLrmsInfo()
    cp = batch.cp
    queueInfo = batch.getQueueInfo()
    totalCpu, freeCpu, queueCpus = batch.parseNodes()
    ce_name = cp_get(cp, ce, "name", "UNKNOWN_CE")
    CE = getTemplate("GlueCE", "GlueCEUniqueID")
    try:
        excludeQueues = [i.strip() for i in cp_get(cp, system_name, \
            "queue_exclude", "").split(',')]
    except:
        excludeQueues = []
    vo_queues = batch.getVoQueues()
    log.info("The following queues were returned from the batch system: %s" % \
        ", ".join(queueInfo))
    for queue, info in queueInfo.items():
        if queue in excludeQueues:
            continue
        info["lrmsVersion"] = version
        info["job_manager"] = system_name
        if info["wait"] > 0:
            info["free_slots"] = 0
        else:
            if queue in queueCpus:
                info["free_slots"] = queueCpus[queue][1]
            else:
                info["free_slots"] = freeCpu
        info["queue"] = queue
        info["ceName"] = ce_name
        unique_id = '%s:2119/jobmanager-%s-%s' % (ce_name, system_name, queue)
        info['ceUniqueID'] = unique_id
        if "job_slots" not in info:
            info['job_slots'] = queueCpus.get(queue, [totalCpu])[0]
        # INVARIANT: job_slots <= totalCpu
        info['job_slots'] = min(info['job_slots'], totalCpu)
        if "priority" not in info:
            info["priority"] = 0
        # note: if max_running == 0, then it was undefined.
        if "max_running" not in info or info['max_running'] == 0:
            info["max_running"] = info["job_slots"]
        if "max_wall" not in info:
            info["max_wall"] = 1440

        ert, wrt = responseTimes(cp, info.get("running", 0),
            info.get("wait", 0), max_job_time=info["max_wall"])

        info['ert'] = ert
        info['wrt'] = wrt
        info['hostingCluster'] = getClusterName(cp)
        info['hostName'] = cp_get(cp, ce, 'host_name', ce_name)
        info['ceImpl'] = 'Globus'
        info['ceImplVersion'] = cp_get(cp, ce, 'globus_version', '4.0.6')
        info['contact_string'] = cp_get(cp, system_name, 'contact_string',
            unique_id)
        info['app_dir'] = cp_get(cp, 'osg_dirs', 'app', "/UNKNOWN_APP")
        info['data_dir'] = cp_get(cp, 'osg_dirs', 'data', "/UNKNOWN_DATA")
        info['default_se'] = getDefaultSE(cp)
        if 'max_waiting' not in info:
            info['max_waiting'] = 999999
        if 'max_queuable' in info:
            info['max_total'] = info['max_queuable']
            # free_slots <= max_queuable
            info['free_slots'] = min(info['free_slots'], info['max_queuable'])
        else:
            if info['max_waiting'] == 999999 or info['max_running'] == 999999:
                info['max_total'] = 999999
            else:
                info['max_total'] = info['max_waiting'] + info['max_running']
            # free_slots <= max_total
            info['free_slots'] = min(info['free_slots'], info['max_total'])
        if 'max_slots' not in info:
            info['max_slots'] = 1

        # INVARIANTS:
        # assigned <= max_running
        info['job_slots'] = min(info['job_slots'], info['max_running'])
        # max_running >= running
        info['max_running'] = max(info['running'], info['max_running'])
        # max_total (jobs in system) >= max_running
        info['max_total'] = max(info['max_total'], info['max_running'])
        # free_slots <= max_running
        info['free_slots'] = min(info['free_slots'], info['max_running'])
        # free_slots <= freeCpu
        info['free_slots'] = min(info['free_slots'], freeCpu)
        # free_slots <= job_slots - running, >= 0
        info['free_slots'] = min(info['free_slots'], info['job_slots'] - \
            info['running'])
        if info['free_slots'] < 0:
            info['free_slots'] = 0
        # job_slots >= running
        info['job_slots'] = max(info['job_slots'], info['running'])

        info['assigned'] = info['job_slots']

        info['lrmsType'] = system_name
        info['preemption'] = cp_get(cp, system_name, 'preemption', '0')
        acbr = ''
        has_vo = False
        for vo, queue2 in vo_queues:
            if queue == queue2:
                acbr += 'GlueCEAccessControlBaseRule: VO:%s\n' % vo
                has_vo = True
        if not has_vo:
            log.info("No VOs in queue %s; not printing." % queue)
            continue
        info['acbr'] = acbr[:-1]
        info['bdii'] = cp.get('bdii', 'endpoint')
        info['gramVersion'] = '2.0'
        info['port'] = 2119
        info['waiting'] = info['wait']
        info['referenceSI00'] = gip_cluster.getReferenceSI00(cp)
        info['clusterUniqueID'] = getClusterID(cp)
        print CE % info
    return queueInfo, totalCpu, freeCpu, queueCpus

def print_VOViewLocal(queue_info, batch):
    system_name, _ = batch.getLrmsInfo()
    cp = batch.cp
    ce_name = cp_get(cp, ce, "name", "UNKNOWN_CE")
    queue_jobs = batch.getJobsInfo()
    VOView = getTemplate("GlueCE", "GlueVOViewLocalID")
    vo_queues = batch.getVoQueues()
    for vo, queue in vo_queues:
        vo_info = queue_jobs.get(queue, {})
        info2 = vo_info.get(vo, {})
        ce_unique_id = '%s:2119/jobmanager-%s-%s' % (ce_name, system_name,
            queue)
        my_queue_info = queue_info.setdefault(queue, {})
        ert, wrt = responseTimes(cp, info2.get("running", 0),
            info2.get("wait", 0),
            max_job_time=my_queue_info.get("max_wall", 0))

        max_running = info2.get('max_running', my_queue_info.get('job_slots',
            0))
        max_running = min(max_running, my_queue_info.get('max_running', 99999))

        info = {
            'ceUniqueID'  : ce_unique_id,
            'job_slots'   : my_queue_info.get('job_slots', 0),
            'free_slots'  : my_queue_info.get('free_slots', 0),
            'ce_name'     : ce_name,
            'queue'       : queue,
            'vo'          : vo,
            'voLocalID'   : vo,
            'job_manager' : system_name,
            'running'     : info2.get('running', 0),
            'max_running' : max_running,
            'priority'    : queue_info.get(queue, {}).get('priority', 0),
            'waiting'     : info2.get('wait', 0),
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
        impl = cp_get(cp, ce, "job_manager", None)
        if impl == 'forwarding':
            batch = Forwarding(cp)
        if impl == 'pbs':
            batch = PbsBatchSystem(cp)
        elif impl == 'condor':
            batch = CondorBatchSystem(cp)
        elif impl == 'sge':
            batch = SgeBatchSystem(cp)
        elif impl == 'lsf':
            batch = LsfBatchSystem(cp)
        else:
            log.error("Unknown job manager: %s" % impl)
            sys.exit(1)
        batch.bootstrap()
        queueInfo, totalCpu, freeCpu, queueCpus = print_CE(batch)
        print_VOViewLocal(queueInfo, batch)
        batch.printAdditional()
    except Exception, e:
        sys.stdout = sys.stderr
        log.error(e)
        raise

if __name__ == '__main__':
    main()

