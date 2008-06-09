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
import xml.dom.minidom
from gip_common import HMSToMin, getLogger, VoMapper, voList
from xml_common import getGipDom, getText
from gip_testing import runCommand

log = getLogger("GIP.SGE")

sge_version_cmd = "qstat -help"
sge_queue_job_info_cmd = 'qstat -f -xml'
sge_queue_list_cmd = 'qconf -sql'
sge_queue_config_cmd = 'qconf -sq %(queue)s'
sge_job_explain_cmd = 'qstat -j %(joblist)s -xml'
# h_rt - hard real time limit (max_walltime)

def sgeCommand(command, cp):
    """
    Execute a command in the shell.  Returns a file-like object
    containing the stdout of the command

    Use this function instead of executing directly (os.popen); this will
    allow you to hook your providers into the testing framework.
    """
    return runCommand(cmd)

def getLrmsInfo(cp):

    for line in sgeCommand(sge_version_cmd, cp):
        return line
    raise Exception("Unable to determine LRMS version info.")

def getQueueInfo(cp):
    """
    Looks up the queue and job information from SGE.

    @param cp: Configuration of site.
    @returns: A dictionary of queue data and a dictionary of job data.
    """
    queueInfo = {}
    jobInfo = {}

    sge_queue_job_info = sgeCommand(sge_queue_job_info_cmd, cp)
    dom = getDom(sge_queue_job_info, sourcetype="string")
    elmQueue_Info = dom.getElementsByTagName("queue-info")
    elmQueueLists = elmQueue_Info.getElementsByTagName("Queue-List")
    for elmQueue in elmQueueLists:
        # get queue name
        name = getText(elmQueue.getElementsByTagName("name"))
        queue = name[:find(name, "@")]

        queue_data = queueInfo.get(queue, {})

        queue_data["queue"] = queue

        used = int(getText(elmQueue.getElementsByTagName("slots_used")))
        total = int(getText(elmQueue.getElementsByTagName("slots_total")))
        free = total - used
        arch = getText(elmQueue.getElementsByTagName("arch"))

        # what about jobs that span slots?
        queue_data["used_slots"] += used
        queue_data["free_slots"] += free
        queue_data["running"] += used
        queue_data["job_slots"] += total
        queue_data["max_running"] += total
        try:
            state = getText(elmQueue.getElementsByTagName("state"))
            if state == "d":
                status = "Draining"
            elif state == "s":
                status = "Closed"
            else:
                status = "Production"
        except:
            status = "Production"

        queue_data["status"] = status
        if status == "Production":
            queue_data["enabled"] = "True"
            queue_data["started"] = "True"
        else:
            queue_data["enabled"] = "False"
            queue_data["started"] = "False"

        queue_data['priority'] = 0  # No such thing that I can find

        # How do you handle queues with no limit?
        sqc = SGEQueueConfig(sgeCommand(sge_queue_config_cmd, cp))
        if sqc['s_rt'].lower() == 'infinity': sqc['s_rt'] = '99999'
        if sqc['h_rt'].lower() == 'infinity': sqc['h_rt'] = '99999'

        if "max_wall" not in queue_data
            queue_data["max_wall"] = min(int(sqc['h_rt']), int(sqc['s_rt']))
        else:
            queue_data["max_wall"] = min(int(sqc['h_rt']), queue_data["max_wall"])
            queue_data["max_wall"] = min(int(sqc['s_rt']), queue_data["max_wall"])

        queueInfo[queue] = queue_data

    return sge_queue_job_info, queueInfo

def getJobInfo(sge_queue_job_info, queueInfo, cp):

    dom = getDom(sge_queue_job_info, sourcetype="string")

    # run through the jobs in pending state
    # find out which queue they belong to and update waiting numbers
    elmJob_Info = dom.getElementsByTagName("job-info")
    elmJobLists = elmJob_Info.getElementsByTagName("job_list")
    job_numbers = ""
    for elmJob in elmJobLists:
        job_numbers = job_numbers + getText(elmJob.getElementsByTagName("JB_job_number")) + ","
    # strip the trailing comma
    job_numbers = job_numbers[-1]

    cmd = sge_job_explain_cmd % {'joblist': job_numbers}
    sge_job_explain = sgeCommand(cmd, cp)
    jobDom = getDom(sge_job_explain, sourcetype="string")
    elmJob_Explain = jobDom.getElementsByTagName("qmaster_response")

    for elmJob in elmJob_Explain:
        # get JB_owner
        owner = getText(elmJob.getElementsByTagName("JB_owner"))
        try:
            vo = vo_map[user].lower()
        except:
            # Most likely, this means that the user is local and not
            # associated with a VO, so we skip the job.
            continue

        # get the queue that the job is waiting for
        queue = getText(elmJob.getElementsByTagName("JB_hard_queue_list")).getElementsByTagName("QR_name")))
        queue_data = queueInfo.get(queue, {})
        queue_data["queue"] = queue

        info = queue_data.get(vo, {"running":0, "wait":0, "total":0})
        # every job listed here is "not running" so they are listed as wait
        info["wait"] += 1
        info["total"] += 1

        queue_data[vo] = info
        queueInfo[queue] = queue_data

    # run through jobs running on queues and update running totals
    elmQueue_Info = dom.getElementsByTagName("queue-info")
    elmQueueLists = elmQueue_Info.getElementsByTagName("Queue-List")

    for elmQueue in elmQueueLists
        name = getText(elmQueue.getElementsByTagName("name"))
        queue = name[:find(name, "@")]
        queue_data = queueInfo.get(queue, {})
        queue_data["queue"] = queue

        try: # if there are no jobs running on the queue, then the joblist won't show up
            elmJobLists = elmQueue.getElementsByTagName("job_list")

            for elmJob in elmJobLists:
                # get JB_owner
                owner = getText(elmJob.getElementsByTagName("JB_owner"))
                try:
                    vo = vo_map[user].lower()
                except:
                    # Most likely, this means that the user is local and not
                    # associated with a VO, so we skip the job.
                    continue
                info = queue_data.get(vo, {"queue":queue, "running":0, "wait":0, "total":0})

                # I don't *think* this check is necessary, but I don't know enough about SGE to be sure
                status = getText(elmJob.getElementsByTagName("state"))
                if status.lower() == "r":
                    info["running"] += 1
                else:
                    # basically, if the job's not running, then we lump it into the waiting status
                    info["wait"] += 1
                info["total"] += 1

                queue_data[vo] = info
                queueInfo[queue] = queue_data
            except:
                # most likely there are no jobs in this queue@execute_host, so the get element operation will fail
                continue

class SGEQueueConfig(QueueConfig):

    def __init__(self, configstring):
        QueueConfig.__init__(self, configstring)

    def digest(self, configstring):
        configList = configstring.split(self.constants.LF)
        for pair in configList:
            key_val = pair.split()
            self[key_val[0].strip()] = key_val[1].strip()
