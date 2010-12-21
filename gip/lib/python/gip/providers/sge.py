#!/usr/bin/python

import re
import sys
import os

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
import gip_cluster
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate, printTemplate, cp_get, cp_getBoolean
from gip_cluster import getClusterID
from gip_sections import ce
from gip_storage import getDefaultSE
from gip_batch import buildCEUniqueID, getGramVersion, getCEImpl, getPort, \
     buildContactString
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

        unique_id = buildCEUniqueID(cp, ce_name, 'sge', queue['name'])

        acbr = ''
        for vo, queue2 in vo_queues:
            if queue['name'] == queue2:
                acbr += 'GlueCEAccessControlBaseRule: VO:%s\n' % vo

        referenceSI00 = gip_cluster.getReferenceSI00(cp)
        contact_string = buildContactString(cp, 'sge', queue['name'], unique_id, log)

        extraCapabilities = ''
	if cp_getBoolean(cp, 'site', 'glexec_enabled', False):
	    extraCapabilities = extraCapabilities + '\n' + 'GlueCECapability: glexec'

        gramVersion = getGramVersion(cp)
        port = getPort(cp)
        ceImpl, ceImplVersion = getCEImpl(cp)

        info = { \
            "ceUniqueID" : unique_id,
            "ceName" : ce_name,
            "ceImpl" : ceImpl,
            "ceImplVersion" : ceImplVersion,
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
            "contact_string" : contact_string,
            "app_dir" : cp_get(cp, 'osg_dirs', 'app', "/OSG_APP_UNKNOWN"),
            "data_dir" : cp_get(cp, 'osg_dirs', 'data', "/OSG_DATA_UNKNOWN"),
            "default_se" : getDefaultSE(cp),
            "max_running" : queue["slots_total"],
            "max_wall" : queue["max_wall"],
            "max_waiting" : default_max_waiting,
            "max_slots" : 1,
            "max_total" : default_max_waiting + queue["slots_total"],
            "assigned" : queue["slots_used"],
            "preemption" : cp_get(cp, 'sge', 'preemption', '0'),
            "acbr" : acbr[:-1],
            "bdii": cp.get('bdii', 'endpoint'),
            "gramVersion" : gramVersion,
            "port" : port,
            "waiting" : queue['waiting'],
            "referenceSI00": referenceSI00,
            'extraCapabilities' : extraCapabilities
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
        ce_unique_id = buildCEUniqueID(cp, ce_name, 'sge', queue)
        info = {
            'ceUniqueID'  : ce_unique_id,
            'voLocalID'   : vo,
            'acbr'        : 'VO:%s' % vo,
            'running'     : queue_jobs.get(queue, {}).get(vo, {}).\
                get('running', 0),
            'waiting'     : queue_jobs.get(queue, {}).get(vo, {}).\
                get('waiting', 0),
            #'free_slots'  : vo.get(queue, {}).get('free_slots', 0),
            'free_slots'  : 0, #TODO: fix
            'ert'         : 3600,
            'wrt'         : 3600,
            'default_se'  : getDefaultSE(cp),
            'app'         : cp_get(cp, "osg_dirs", "app", "/OSG_APP_UNKNOWN"),
            'data'        : cp_get(cp, "osg_dirs", "data", "/OSG_DATA_UNKNOWN"),
        }
        info['total'] = info['waiting'] + info['running']
        printTemplate(VOView, info)

def bootstrapSGE(cp):
    """
    If it exists, source
    $SGE_ROOT/$SGE_CELL/common/settings.sh
    """
    sge_root = cp_get(cp, "sge", "sge_root", "")
    if not sge_root:
        log.warning("Could not locate sge_root in config file!  Not " \
            "bootstrapping SGE environment.")
        return
    sge_cell = cp_get(cp, "sge", "sge_cell", "")
    if not sge_cell:
        log.warning("Could not locate sge_cell in config file!  Not " \
            "bootstrapping SGE environment.")
        return
    settings = os.path.join(sge_root, sge_cell, "common/settings.sh")
    if not os.path.exists(settings):
        log.warning("Could not find the SGE settings file; looked in %s" % \
            settings)
        return
    cmd = "/bin/sh -c 'source %s; /usr/bin/env'" % settings
    fd = os.popen(cmd)
    results = fd.read()
    if fd.close():
        log.warning("Unable to source the SGE settings file; tried %s." % \
            settings)
    for line in results.splitlines():
        line = line.strip()
        info = line.split('=', 2)
        if len(info) != 2:
            continue
        os.environ[info[0]] = info[1]

def main():
    try:
        cp = config()
        bootstrapSGE(cp)
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

