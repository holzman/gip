
"""
Module for interacting with SGE.
"""

import re
import os
import sys
from gip_common import HMSToMin, getLogger, VoMapper, voList
from gip_testing import runCommand

log = getLogger("GIP.SGE")

batch_system_info_cmd = "sge_schedd --help"
queue_info_cmd = "qstat -f -xml %(sgeHost)s"
jobs_cmd = "qstat -xml %(sgeHost)s"
sgenodes_cmd = "sgenodes -a"


def sgeCommand(command, cp):
    """
    Execute a command in the shell.  Returns a file-like object
    containing the stdout of the command

    Use this function instead of executing directly (os.popen); this will
    allow you to hook your providers into the testing framework.
    """

    sgeHost = cp.get("sge", "host")
    if sgeHost.lower() == "none" or sgeHost.lower() == "localhost":
        sgeHost = ""
    cmd = command % {'sgeHost': sgeHost}

    if info:
        cmd = command % info
    else:
        cmd = command

    return runCommand(cmd)

def getLrmsInfo(cp):
    version_re = re.compile("GE\s+(\S+)")
    for line in sgeCommand(batch_system_info_cmd, cp):
        m = version_re.search(line)
        if m:
            return m.groups()[0]
    raise Exception("Unable to determine LRMS version info.")

def getJobsInfo(vo_map, cp):
    queue_jobs = {}
    for orig_line in lsfCommand(jobs_cmd, cp):
        try:
            job, name, user, time, status, queue = orig_line.split()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            continue
        if job.startswith("-"):
            continue
        queue_data = queue_jobs.get(queue, {})
        try:
            vo = vo_map[user].lower()
        except:
            # Most likely, this means that the user is local and not
            # associated with a VO, so we skip the job.
            continue
        info = queue_data.get(vo, {"running":0, "wait":0, "total":0})
        if status == "R":
            info["running"] += 1
        else:
            info["wait"] += 1
        info["total"] += 1
        queue_data[vo] = info
        queue_jobs[queue] = queue_data
    return queue_jobs

def getQueueInfo(cp):
    """
    Looks up the queue information from SGE.
    
    This is an almost direct copy from PBS, MUST CHANGE!!!

    The returned dictionary contains the following keys:

      - B{status}: Production, Queueing, Draining, Closed
      - B{priority}: The priority of the queue.
      - B{max_wall}: Maximum wall time.
      - B{max_running}: Maximum number of running jobs.
      - B{running}: Number of running jobs in this queue.
      - B{wait}: Waiting jobs in this queue.
      - B{total}: Total number of jobs in this queue.

    @param cp: Configuration of site.
    @returns: A dictionary of queue data.  The keys are the queue names, and
        the value is the queue data dictionary.
    """
    queueInfo = {}
    queue_data = None
    for orig_line in sgeCommand(queue_info_cmd, cp):
        line = orig_line.strip()
        if line.startswith("Queue: "):
            if queue_data != None:
                if queue_data["started"] and queue_data["enabled"]:
                    queue_data["status"] = "Production"
                elif queue_data["enabled"]:
                    queue_data["status"] = "Queueing"
                elif queue_data["started"]:
                    queue_data["status"] = "Draining"
                else:
                    queue_data["status"] = "Closed"
                del queue_data["started"]
                del queue_data['enabled']
            queue_data = {}
            queue_name = line[7:]
            queueInfo[queue_name] = queue_data
            continue
        if queue_data == None:
            continue
        if len(line) == 0:
            continue
        attr, val = line.split(" = ")
        if attr == "Priority":
            queue_data['priority'] = int(val)
        elif attr == "total_jobs":
            queue_data["total"] = int(val)
        elif attr == "state_count":
            info = val.split()
            for entry in info:
                state, count = entry.split(':')
                count = int(count)
                if state == 'Queued':
                    queue_data['wait'] = queue_data.get('wait', 0) + count
                #elif state == 'Waiting':
                #    queue_data['wait'] = queue_data.get('wait', 0) + count
                elif state == 'Running':
                    queue_data['running'] = count
        elif attr == "resources_max.walltime":
            queue_data["max_wall"] = HMSToMin(val)
        elif attr == "enabled":
            queue_data["enabled"] = val == "True"
        elif attr == "started":
            queue_data["started"] = val == "True"
        elif attr == "max_running":
            queue_data["max_running"] = int(val)
        elif attr == "resources_max.nodect":
            queue_data["job_slots"] = int(val)
    if queue_data != None:
        if queue_data["started"] and queue_data["enabled"]:
            queue_data["status"] = "Production"
        elif queue_data["enabled"]:
            queue_data["status"] = "Queueing"
        elif queue_data["started"]:
            queue_data["status"] = "Draining"
        else:
            queue_data["status"] = "Closed"
        del queue_data["started"]
        del queue_data['enabled']

    return queueInfo

def parseNodes(cp, version):
    """
    Parse the node information from SGE.  Using the output from sgenodes,
    determine:

        - The number of total CPUs in the system.
        - The number of free CPUs in the system.
        - A dictionary mapping SGE queue names to a tuple containing the
            (totalCPUs, freeCPUs).
    """
    totalCpu = 0
    freeCpu = 0
    queueCpu = {}
    queue = None
    avail_cpus = None
    used_cpus = None
    for line in sgeCommand(sgenodes_cmd, cp):
        try:
            attr, val = line.split(" = ")
        except:
            continue
        if attr == "state":
            state = val
        if attr == "np":
            try:
                np = int(val)
            except:
                np = 1
            if not (state.find("down") >= -1 or \
                    state.find("offline") >= -1):
                totalCpu += np
            if state.find("free") >= -1:
                freeCpu += np
        if attr == "jobs" and state == "free":
            freeCpu -= val.count(',')

    return totalCpu, freeCpu, queueCpu

def getQueueList(cp):
    """
    Returns a list of all the queue names that are supported.

    @param cp: Site configuration
    @returns: List of strings containing the queue names.
    """
    queues = []
    try:
        queue_exclude = [i.strip() for i in cp.get("sge", "queue_exclude").\
            split(',')]
    except:
        queue_exclude = []
    for queue in getQueueInfo(cp):
        if queue not in queue_exclude:
            queues.append(queue)
    return queues

def getVoQueues(cp):
    """
    Determine the (vo, queue) tuples for this site.  This allows for central
    configuration of which VOs are advertised.

    Sites will be able to blacklist queues they don't want to advertise,
    whitelist certain VOs for a particular queue, and blacklist VOs from queues.

    @param cp: Site configuration
    @returns: A list of (vo, queue) tuples representing the queues each VO
        is allowed to run in.
    """
    voMap = VoMapper(cp)
    try:
        queue_exclude = [i.strip() for i in cp.get("sge", "queue_exclude").\
            split(',')]
    except:
        queue_exclude = []
    vo_queues= []
    for queue in getQueueInfo(cp):
        if queue in queue_exclude:
            continue
        try:
            whitelist = [i.strip() for i in cp.get("sge", "%s_whitelist" % \
                queue).split(',')]
        except:
            whitelist = []
        try:
            blacklist = [i.strip() for i in cp.get("sge", "%s_blacklist" % \
                queue).split(',')]
        except:
            blacklist = []
        for vo in voList(cp, voMap):
            if (vo in blacklist or "*" in blacklist) and ((len(whitelist) == 0)\
                    or vo not in whitelist):
                continue
            vo_queues.append((vo, queue))
    return vo_queues

