# Tony's notes
#
# CE must have SGE_ROOT mounted
#
# get list of queues: qconf -sql
# get queue properties: qconf -sq queue_name
# Master hostname is in <sge_root>/<cell>/common/act_qmaster
# get list of execution hosts: qconf -sel
# get list of requestable attributes: qconf -scl
# get list of ACL's: qconf -sul
#

"""
Module for interacting with SGE.
"""

import re
import os
import sys

from gip_common import  getLogger, VoMapper, voList
from xml_common import parseXmlSax
from sge_sax_handler import QueueInfoParser
from gip_testing import runCommand
from UserDict import UserDict

log = getLogger("GIP.SGE")

sge_version_cmd = "qstat -help"
sge_queue_info_cmd = 'qstat -f -xml'
sge_queue_config_cmd = 'qconf -sq %s'
sge_job_info_cmd = 'qstat -xml'
sge_queue_list_cmd = 'qconf -sql'

# h_rt - hard real time limit (max_walltime)


def getLrmsInfo(cp):
    for line in runCommand(sge_version_cmd, cp):
        return line.strip('\n')
    raise Exception("Unable to determine LRMS version info.")

def getQueueInfo(cp):
    """
    Looks up the queue and job information from SGE.

    @param cp: Configuration of site.
    @returns: A dictionary of queue data and a dictionary of job data.
    """
    queue_list = {}
    xml = runCommand(sge_queue_info_cmd, cp)
    handler = QueueInfoParser()
    parseXmlSax(xml, handler)
    queue_info = handler.getQueueInfo()
    for queue in queue_info:
        if queue == 'waiting':
            continue

        # get queue name
        name = queue.split("@")[0]
        q = queue_list.get(name, {'slots_used': 0, 'slots_total': 0, 'slots_free': 0, 'waiting' : 0, 'name' : name})
        q['slots_used'] += int(queue_info[queue]['slots_used'])
        q['slots_total'] += int(queue_info[queue]['slots_total'])
        q['slots_free'] = q['slots_total'] - q['slots_used']
        q['arch'] = queue_info[queue]["arch"]
        q['max_running'] = q['slots_total']

        try:
            state = queue_info[queue]["state"]
            if state == "d":
                status = "Draining"
            elif state == "s":
                status = "Closed"
            else:
                status = "Production"
        except:
            status = "Production"

        q['status'] = status
        q['priority'] = 0  # No such thing that I can find for a queue

        # How do you handle queues with no limit?
        sqc = SGEQueueConfig(runCommand(sge_queue_config_cmd % queue, cp).read())
        if sqc['s_rt'].lower() == 'infinity': sqc['s_rt'] = '99999'
        if sqc['h_rt'].lower() == 'infinity': sqc['h_rt'] = '99999'
        max_wall = min(sqc['s_rt'], sqc['h_rt'])

        try:
            q['max_wall'] = min(max_wall, q['max_wall'])
        except:
            q['max_wall'] = max_wall

        queue_list[name] = q

    waiting_jobs = 0
    for job in queue_info['waiting']:
        waiting_jobs += 1
    queue_list['waiting'] = waiting_jobs

    return queue_list, queue_info

def getJobsInfo(sge_queue_job_info, queueInfo, cp):
    xml = runCommand(sge_queue_info_cmd, cp)
    handler = JobInfoParser()
    parseXmlSax(xml, handler)
    job_info = handler.getJobInfo()
    queue_jobs = {}

    for job in job_info:
        user = job_info['JB_owner']
        state = job_info['state']
        try:
            vo = vo_map[user].lower()
        except:
            # Most likely, this means that the user is local and not
            # associated with a VO, so we skip the job.
            continue

        info = queue_jobs.get(vo, {"running":0, "wait":0, "total":0})
        if stat3 == "r":
            info["running"] += 1
        else:
            info["wait"] += 1
        info["total"] += 1
        info["vo"] = vo
        queue_jobs[vo] = info
    return queue_jobs

class SGEQueueConfig(UserDict):
    def __init__(self, configstring):
        from gip_common import _Constants
        UserDict.__init__(self, dict=None)
        self.constants = _Constants()
        self.digest(configstring)

    def digest(self, configstring):
        configList = configstring.split(self.constants.LF)
        for pair in configList:
            if len(pair) > 1:
                key_val = pair.split()
                self[key_val[0].strip()] = key_val[1].strip()

def getVoQueues(cp):
    voMap = VoMapper(cp)
    try:
        queue_exclude = [i.strip() for i in cp.get("sge", "queue_exclude").split(',')]
    except:
        queue_exclude = []
    vo_queues = []
    queue_list, q = getQueueInfo(cp)
    for queue in queue_list:
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

        for vo in voList(cp, voMap):
            if (vo in blacklist or "*" in blacklist) and ((len(whitelist) == 0) or vo not in whitelist):
                continue
            vo_queues.append((vo, queue))

    return vo_queues
