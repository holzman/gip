#!/usr/bin/python

"""
Print out GLUE describing the local LSF batch system.
"""

import sys
import os

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
import gip_cluster
from gip_testing import runCommand
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate, \
    printTemplate, cp_get, cp_getInt, responseTimes, cp_getBoolean
from gip_cluster import getClusterID
from lsf_common import parseNodes, getQueueInfo, getJobsInfo, getLrmsInfo, \
    getVoQueues
from gip_sections import ce
from gip_storage import getDefaultSE

log = getLogger("GIP.LSF")

def print_CE(cp):
    """
    Print out the GlueCE objects for LSF; one GlueCE per grid queue.
    """
    try:
        lsfVersion = getLrmsInfo(cp)
    except:
        lsfVersion = 'Unknown'

    log.debug('Using LSF version %s' % lsfVersion)    
    queueInfo = getQueueInfo(cp)
    try:
        totalCpu, freeCpu, queueCpus = parseNodes(queueInfo, cp)
    except:
        #raise
        totalCpu, freeCpu, queueCpus = 0, 0, {}
    log.debug('Total, Free CPU: (%s, %s)' % (totalCpu, freeCpu))
    ce_name = cp.get(ce, "name")
    CE = getTemplate("GlueCE", "GlueCEUniqueID")
    try:
        excludeQueues = [i.strip() for i in cp.get("lsf", \
            "queue_exclude").split(',')]
    except:
        excludeQueues = []
    vo_queues = getVoQueues(queueInfo, cp)
    for queue, info in queueInfo.items():
        if queue in excludeQueues:
            continue
        log.debug('Processing queue %s' % queue)
        if 'running' not in info:
            info['running'] = 0
        if 'status' not in info:
            # There really should be an unknown status...
            info['status'] = 'Closed'
        if 'total' not in info:
            info['total'] = 0
        info["lrmsVersion"] = lsfVersion
        info["job_manager"] = "lsf"
        if int(info.get("wait", 0)) > 0:
            info["free_slots"] = 0
        else:
            if queue in queueCpus and 'max' in queueCpus[queue] and 'njobs' in queueCpus[queue]:
                info["free_slots"] = queueCpus[queue]['max'] - queueCpus[queue]['njobs']
            else:
                info["free_slots"] = freeCpu
        info["queue"] = queue
        info["ceName"] = ce_name
        unique_id = '%s:2119/jobmanager-lsf-%s' % (ce_name, queue)
        info['ceUniqueID'] = unique_id
        if "job_slots" not in info:
            if queue in queueCpus and 'max' in queueCpus[queue]:
                log.debug('queue %s, info is %s' % (queue, queueCpus[queue]))
                info['job_slots'] = queueCpus[queue]['max']
            else:
                info["job_slots"] = totalCpu
        if "priority" not in info:
            info["priority"] = 0
        if "max_running" not in info:
            info["max_running"] = info["job_slots"]
        elif not info['max_running'] or info['max_running'] == '-':
            info['max_running'] = 999999
        if "max_wall" not in info:
            info["max_wall"] = 1440
        info["max_wall"] = int(info["max_wall"]) # glue proscribes ints 
        info["job_slots"] = min(totalCpu, info["job_slots"])

        ert, wrt = responseTimes(cp, info["running"], info["wait"],
            max_job_time=info["max_wall"])

        contact_string = cp_get(cp, "lsf", 'job_contact', unique_id)
        if contact_string.endswith("jobmanager-lsf"):
            contact_string += "-%s" % queue

        info['ert'] = ert
        info['wrt'] = wrt
        info['hostingCluster'] = cp_get(cp, ce, 'hosting_cluster', ce_name)
        info['hostName'] = cp_get(cp, ce, 'host_name', ce_name)
        info['ceImpl'] = 'Globus'
        info['ceImplVersion'] = cp_get(cp, ce, 'globus_version', '4.0.6')
        info['contact_string'] = contact_string
        info['app_dir'] = cp.get('osg_dirs', 'app')
        info['data_dir'] = cp.get('osg_dirs', 'data')
        info['default_se'] = getDefaultSE(cp)
        info['max_waiting'] = 999999
        info['max_slots'] = 1
        #info['max_total'] = info['max_running']
        info['max_total'] = info['max_waiting'] + info['max_running']
        info['assigned'] = info['job_slots']
        info['lrmsType'] = 'lsf'
        info['preemption'] = str(cp_getInt(cp, 'lsf', 'preemption', '0'))
        acbr = ''
        for vo, queue2 in vo_queues:
            if queue == queue2:
                acbr += 'GlueCEAccessControlBaseRule: VO:%s\n' % vo.lower()
        if not acbr:
            continue
        #print info
        info['acbr'] = acbr[:-1]
        info['bdii'] = cp.get('bdii', 'endpoint')
        gramVersion = ''
        if not cp_getBoolean('cream', 'enabled', False):
            gramVersion = '\n' + 'GlueCEInfoGRAMVersion: 2.0'
        info['gramVersion'] = gramVersion
        info['port'] = 2119
        info['waiting'] = info.get('wait', 0)
        info['referenceSI00'] = gip_cluster.getReferenceSI00(cp)
        info['clusterUniqueID'] = getClusterID(cp)

        extraCapabilities = ''
        if cp_getBoolean('site', 'glexec_enabled', False):
            extraCapabilities = extraCapabilities + '\n' + 'GlueCECapability: glexec'
        info['extraCapabilities'] = extraCapabilities
        
        printTemplate(CE, info)
    return queueInfo, totalCpu, freeCpu, queueCpus

def print_VOViewLocal(queue_info, cp):
    """
    Print out the VOView objects for the LSF batch system.
    
    One VOView per VO per queue, for each VO which has access
    to the queue.
    """
    ce_name = cp.get(ce, "name")
    vo_map = VoMapper(cp)
    queue_jobs = getJobsInfo(vo_map, cp)
    VOView = getTemplate("GlueCE", "GlueVOViewLocalID")
    vo_queues = getVoQueues(queue_info, cp)
    for vo, queue in vo_queues:
        vo = vo.lower()
        vo_info = queue_jobs.get(queue, {})
        info2 = vo_info.get(vo, {})
        ce_unique_id = '%s:2119/jobmanager-lsf-%s' % (ce_name, queue)

        my_queue_info = queue_info.setdefault(queue, {})
        if "max_wall" not in my_queue_info:
            my_queue_info["max_wall"] = 1440
        ert, wrt = responseTimes(cp, info2.get("running", 0),
            info2.get("waiting", 0),
            max_job_time=my_queue_info.get("max_wall", 0))

        free_slots = my_queue_info.get('free_slots', 0)
        waiting = info2.get('waiting', 0)
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
            'job_manager' : 'lsf',
            'running'     : info2.get('running', 0),
            'max_running' : info2.get('max_running', 0),
            'priority'    : queue_info.get(queue, {}).get('priority', 0),
            'waiting'     : waiting,
            'data'        : cp.get("osg_dirs", "data"),
            'app'         : cp.get("osg_dirs", "app"),
            'default_se'  : getDefaultSE(cp),
            'ert'         : ert,
            'wrt'         : wrt,
            'acbr'        : 'VO:%s' % vo
        }
        info['total'] = info['waiting'] + info['running']
        printTemplate(VOView, info)

def bootstrapLSF(cp):
    """
    If it exists, source the profile.lsf file
    """
    lsf_profile = cp_get(cp, "lsf", "lsf_profile", "/lsf/conf/profile.lsf")
    if not os.path.exists(lsf_profile):
        log.warning("Could not find the LSF profile file; looked in %s" %
                    lsf_profile)
        return

    log.debug('Executing lsf profile from %s' % lsf_profile)
    cmd = "/bin/sh -c 'source %s; /usr/bin/env'" % lsf_profile
    output = runCommand(cmd)
    for line in output.readlines():
        line = line.strip()
        info = line.split('=', 2)
        if len(info) != 2:
            continue
        os.environ[info[0]] = info[1]
    
def main():
    """
    Wrapper for printing out the LSF-related GLUE objects.
    """
    log.debug("Beginning LSF provider")
    try:
        cp = config()
        lsf_path = cp_get(cp, "lsf", "lsf_location", None)
        if lsf_path:
            addToPath(lsf_path)
        bootstrapLSF(cp)
        #vo_map = VoMapper(cp)
        queueInfo, _, _, _ = print_CE(cp)
        print_VOViewLocal(queueInfo, cp)
    except Exception, e:
        sys.stdout = sys.stderr
        log.exception(e)
        raise

if __name__ == '__main__':
    main()

