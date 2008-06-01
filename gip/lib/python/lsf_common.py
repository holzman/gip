
"""
Module for interacting with LSF.
"""

import re
import os
import sys
import sets
from gip_common import HMSToMin, getLogger, VoMapper, voList
from gip_testing import runCommand

log = getLogger("GIP.LSF")

queue_info_cmd = "bqueues -l"
jobs_cmd = "bjobs -u all"
lsfnodes_cmd = "bhosts"
lsid_cmd = 'lsid'
bmgroup_r_cmd = 'bmgroup -r'
bugroup_r_cmd = 'bugroup -r'

def lsfCommand(command, cp):
    lsfHost = cp_get(cp, "lsf", "host", "localhost")
    if lsfHost.lower() == "none" or lsfHost.lower() == "localhost":
        lsfHost = ""
    cmd = command % {'lsfHost': lsfHost}
    fp = runCommand(cmd)
    return fp

def getLrmsInfo(cp):
    version_re = re.compile("Platform LSF")
    for line in lsfCommand(lsid_cmd, cp):
        m = version_re.search(line)
        if m:
            return line.strip()
    raise Exception("Unable to determine LRMS version info.")

def getUserGroups(cp):
    groups = {}
    for line in lsfCommand(bugroup_r_cmd, cp):
        line = line.strip()
        info = line.split()
        group = info[0]
        users = info[1:]
        groups[group] = users
    return groups

def usersToVos(qInfo, user_groups, vo_map):
    users = qInfo['USERS']
    all_users = []
    for user in users.split():
        if user.endswith('/'):
            all_users += user_groups.get(user[:-1], [])
        else:
            all_users.append(user)
    all_vos = sets.Set()
    for user in users:
        try:
            vo = vo_map[user]
        except:
            continue
        all_vos.add(vo)
    return list(all_vos)

def getJobsInfo(vo_map, cp):
    queue_jobs = {}
    for orig_line in lsfCommand(jobs_cmd, cp):
        line = orig_line.strip()
        try:
            info = line.split()
            job, user, stat, queue, from_host, exec_host = info[:6]
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            continue
        if job == 'JOBID':
            continue
        queue_data = queue_jobs.get(queue, {})
        try:
            vo = vo_map[user].lower()
        except:
            # Most likely, this means that the user is local and not
            # associated with a VO, so we skip the job.
            continue
        info = queue_data.get(vo, {"running":0, "wait":0, "total":0})
        if status == "RUN":
            info["running"] += 1
        elif status == 'PEND':
            info["wait"] += 1
        info["total"] += 1
        queue_data[vo] = info
        queue_jobs[queue] = queue_data
    return queue_jobs

def getQueueInfo(cp):
    """
    Looks up the queue information from LSF.
    
    The keys of the returned dictionary are the queue names, and the value is
    the queue data directory. 

    The queue data dictionary contains the following keys:

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
    statistics_re = '\s*(.*?)\s+(.*?)\s+(.*?)\s+(.*?)\s+(.*?)\s+(.*?)\s+' \
        '(.*?)\s+(.*?)\s+(.*?)\s+(.*?)\s+(.*?)\s+(.*?)\s+(.*?)\s+'
    statistics_re = re.compile(statistics_re)
    attribute_re = re.compile('(\S*):\s+(\S*?)\s*')
    limits_re = re.compile('(.*)\s+LIMITS:')
    queue = None
    limits = None
    limit_key = None
    for orig_line in lsfCommand(queue_info_cmd, cp):
        line = orig_line.strip()
        # Skip blank lines
        if len(line) == 0:
            continue
        # Queue attribute
        m = attribute_re.match(orig_line)
        if m:
            key, val = m.groups()
            if key == 'QUEUE':
                queue = val
                if queue not in queueInfo:
                    queueInfo[queue] = {}
                qInfo = queueInfo[queue]
            qInfo[key] = val
            continue
        if not queue:
            continue
        # Queue statistics line
        m = statistics_re.match(orig_line)
        if m:
            prio, nice, status, max, _, _, _, njobs, pend, run, _, _, _ = \
                m.groups()
            if prio == 'PRIO': # Header line
                continue
            qInfo['priority'] = prio
            qInfo['nice'] = nice
            qInfo['total'] = njobs
            qInfo['wait'] = pend
            qInfo['running'] = run
            qInfo['max_running'] = max
            if status == 'Open:Active':
                qInfo['status'] = "Production"
            elif status == 'Open:Inact':
                qInfo['status'] = "Queueing"
            elif status == 'Closed:Active':
                qInfo['status'] = 'Draining'
            elif status == 'Closed:Inact':
                qInfo['status'] = 'Closed'
            continue

        # We are left with parsing limits data
        m = limits_re.match(line)
        if m:
            limits = m.groups()[0]
            continue
        if limits != 'MAXIMUM':
            continue
        if not limit_key:
            limit_key = line
            continue
        try:
            val = float(line.split()[0])
            qInfo[limit_key] = val
            if limit_key == 'RUNLIMIT':
                qInfo['max_wall'] = val
            elif limit_key == 'PROCLIMIT':
                qInfo['max_processors'] = int(val)
        except:
            pass
        limit_key = None
    
    user_groups = getUserGroups(cp)
    for queue, qInfo in queueInfo.items():
        qInfo['vos'] = usersToVos(qInfo, user_groups, vo_map)
    return queueInfo

def parseNodes(queueInfo, cp):
    """
    Parse the node information from LSF.  Using the output from bhosts,
    determine:

        - The number of total CPUs in the system.
        - The number of free CPUs in the system.
        - A dictionary mapping LSF queue names to a tuple containing the
            (totalCPUs, freeCPUs).

    @param queueInfo: The information about the queues, as returned by
       getQueueInfo
    @param cp: ConfigParser object holding the GIP configuration.
    """
    totalCpu = 0
    freeCpu = 0
    queueCpu = {}
    bhosts_re = re.compile('(.?*)\s+(.?*)\s+(.?*)\s+(.?*)\s+(.?*)\s+(.?*)\s+' \
        '(.?*)\s+(.?*)\s+(.?*)')
    hostInfo = {}
    for line in lsfCommand(lsfnodes_cmd, cp):
        m = bhosts_re.match(line)
        if not m:
            continue
        host, status, jl_u, max, njobs, run, ssusp, ususp, rsv = m.groups()
        if host == 'HOST_NAME':
            continue
        if status == 'unavail' or status == 'unreach':
            continue
        hostInfo[host] = {'max': max, 'njobs': njobs}
        totalCpu += max
        freeCpu += njobs
    groupInfo = {}
    for line in lsfCommand(bmgroup_r_cmd, cp):
        info = line.strip().split()
        groupInfo[info[0]] = info[1:]
    for queue, qInfo in queueInfo.items():
        max, njobs = 0, 0
        for group in qInfo['HOSTS']:
            for host in groupInfo[group]:
                hInfo = hostInfo.get(host, {})
                max += hInfo.get('max', 0)
                njobs += hInfo.get('njobs', 0)
        queueCpu[queue] = {'max': max, 'njobs': njobs}
    return totalCpu, freeCpu, queueCpu

def getQueueList(queueInfo, cp):
    """
    Returns a list of all the queue names that are supported.

    @param cp: Site configuration
    @returns: List of strings containing the queue names.
    """
    queues = []
    try:
        queue_exclude = [i.strip() for i in cp.get("lsf", "queue_exclude").\
            split(',')]
    except:
        queue_exclude = []
    for queue in queueInfo:
        if queue not in queue_exclude:
            queues.append(queue)
    return queues

def getVoQueues(queueInfo, cp):
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
        queue_exclude = [i.strip() for i in cp.get("lsf", "queue_exclude").\
            split(',')]
    except:
        queue_exclude = []
    vo_queues= []
    for queue in queueInfo:
        if queue in queue_exclude:
            continue
        try:
            whitelist = [i.strip() for i in cp.get("lsf", "%s_whitelist" % \
                queue).split(',')]
        except:
            whitelist = []
        try:
            blacklist = [i.strip() for i in cp.get("lsf", "%s_blacklist" % \
                queue).split(',')]
        except:
            blacklist = []
        for vo in queueInfo[queue].get('vos', voList(cp, voMap)):
            if (vo in blacklist or "*" in blacklist) and ((len(whitelist) == 0)\
                    or vo not in whitelist):
                continue
            vo_queues.append((vo, queue))
    return vo_queues

