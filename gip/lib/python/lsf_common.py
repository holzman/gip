
"""
Module for interacting with LSF.

Originally developed using CERN as a test case.
"""

import re
import os
import sys
import sets
from gip_common import HMSToMin, getLogger, VoMapper, voList, cp_get, parseRvf
from gip_testing import runCommand

log = getLogger("GIP.LSF")

queue_info_cmd = "bqueues -l"
jobs_cmd = "bjobs -u all -r"
lsfnodes_cmd = "bhosts"
lsid_cmd = 'lsid'
bmgroup_r_cmd = 'bmgroup -r'
bugroup_r_cmd = 'bugroup -r'

def lsfCommand(command, cp):
    """
    Run a command for the LSF batch system

    Config options used:
       * lsf.host.  The LSF server hostname.  Defaults to localhost

    @returns: File-like object of the LSF output.
    """
    lsfHost = cp_get(cp, "lsf", "host", "localhost")
    if lsfHost.lower() == "none" or lsfHost.lower() == "localhost":
        lsfHost = ""
    cmd = command % {'lsfHost': lsfHost}
    fp = runCommand(cmd)
    return fp

def getLrmsInfo(cp):
    """
    Get the version information about the LSF version.

    @returns: The LSF version string.
    @throws Excaeption: General exception thrown if version string can't
        be determined.
    """
    version_re = re.compile("Platform LSF")
    for line in lsfCommand(lsid_cmd, cp):
        m = version_re.search(line)
        if m:
            return line.strip()
    raise Exception("Unable to determine LRMS version info.")

def getUserGroups(cp):
    """
    Get a list of groups and the users in the groups, using bugroup -r.

    @returns: A dictionary of user-groups; the keys are the group name,
        the value is a list of user names
    """
    groups = {}
    for line in lsfCommand(bugroup_r_cmd, cp):
        line = line.strip()
        info = line.split()
        group = info[0]
        users = info[1:]
        groups[group] = users
    return groups

def usersToVos(cp, qInfo, user_groups, vo_map):
    """
    Given a queue's information, determine the users allowed to access the
    queue.

    @param qInfo: Information about a LSF queue; we assume that qInfo['USERS']
       is a string of users/groups allowed to access the queue.
    @param user_groups: A mapping of group name to a list of user names.
    @param vo_map: A mapping of user name to vo name.
    @returns: List of all the VOs allowed to access this queue.
    """
    users = qInfo['USERS']
    all_users = []
    for user in users.split():
        if user.endswith('/'):
            all_users += user_groups.get(user[:-1], [])
        elif user == 'all':
            all_users.append(None)
        else:
            all_users.append(user)
    all_vos = sets.Set()
    if None in all_users:
        return voList(cp, vo_map)
    for user in all_users:
        try:
            vo = vo_map[user]
        except:
            continue
        all_vos.add(vo)
    return list(all_vos)

def getJobsInfo(vo_map, cp):
    """
    @param vo_map: A mapping of user name to vo name.
    @param cp: The GIP configuration object
    """
    queue_jobs = {}
    for orig_line in lsfCommand(jobs_cmd, cp):
        line = orig_line.strip()
        try:
            info = line.split()
            job, user, status, queue, from_host, exec_host = info[:6]
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
    attribute_re = re.compile('(\S*):\s+(.*)\s*')
    limits_re = re.compile('(.*)\s+LIMITS:')
    queue = None
    limits = None
    limit_key = None
    hasQueueInfoHeader = False # Set to true when we have found the header line
        # for the PARAMETERS/STATISTICS data.
    hasQueueInfo = False # Set to true when we have found the values in the
        # PARAMETERS/STATISTICS data.
    for orig_line in lsfCommand(queue_info_cmd, cp):
        line = orig_line.strip()
        # Skip blank lines
        if len(line) == 0:
            continue
        # Queue attribute line.  Something like this:
        # JOB_STARTER:  /usr/local/lsf/etc/job_starter '%USRCMD'
        m = attribute_re.match(orig_line)
        if m:
            key, val = m.groups()
            # The line starting the new queue information is something like:
            # QUEUE: grid_ops
            if key == 'QUEUE':
                queue = val
                hasQueueInfo, hasQueueInfoHeader = False, False
                if queue not in queueInfo:
                    queueInfo[queue] = {}
                qInfo = queueInfo[queue]
            qInfo[key] = val
            continue
        if not queue:
            continue
        # Queue statistics line
        m = statistics_re.match(orig_line)
        if m and not hasQueueInfo:
            prio, nice, status, max, _, _, _, njobs, pend, run, _, _, _ = \
                m.groups()
            if prio == 'PRIO': # Header line, no useful information
                hasQueueInfoHeader = True
                continue
            # Make sure we only parse the data *after* the header
            if not hasQueueInfoHeader:
                continue
            qInfo['priority'] = prio
            qInfo['nice'] = nice
            qInfo['total'] = njobs
            qInfo['wait'] = pend
            qInfo['running'] = run
            try:
                qInfo['max_running'] = int(max)
            except:
                qInfo['max_running'] = 999999
            if status == 'Open:Active':
                qInfo['status'] = "Production"
            elif status == 'Open:Inact':
                qInfo['status'] = "Queueing"
            elif status == 'Closed:Active':
                qInfo['status'] = 'Draining'
            elif status == 'Closed:Inact':
                qInfo['status'] = 'Closed'
            hasQueueInfo = True
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
   
    vo_map = VoMapper(cp) 
    user_groups = getUserGroups(cp)
    for queue, qInfo in queueInfo.items():
        qInfo['vos'] = usersToVos(cp, qInfo, user_groups, vo_map)
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
    hostInfo = {}
    for line in lsfCommand(lsfnodes_cmd, cp):
        info = [i.strip() for i in line.split()]
        # Skip any malformed lines
        if len(info) != 9:
            continue
        host, status, jl_u, max, njobs, run, ssusp, ususp, rsv = info
        if host == 'HOST_NAME':
            continue
        if status == 'unavail' or status == 'unreach':
            continue
        try:
            max = int(max)
        except:
            max = 0
        try:
            njobs = int(njobs)
        except:
            njobs = 0
        hostInfo[host] = {'max': max, 'njobs': njobs}
        totalCpu += max
        freeCpu += max-njobs
    groupInfo = {}
    for line in lsfCommand(bmgroup_r_cmd, cp):
        info = line.strip().split()
        groupInfo[info[0]] = info[1:]
    for queue, qInfo in queueInfo.items():
        max, njobs = 0, 0
        for group in qInfo['HOSTS'].split():
            for host in groupInfo.get(group[:-1], []):
                hInfo = hostInfo.get(host, {})
                max += hInfo.get('max', 0)
                njobs += hInfo.get('njobs', 0)
        queueCpu[queue] = {'max': max, 'njobs': njobs}
    return totalCpu, freeCpu, queueCpu

def getQueueList(cp):
    """
    Returns a list of all the queue names that are supported.

    Config options used:
        * lsf.queue_exclude
    
    @param cp: Site configuration
    @returns: List of strings containing the queue names.
    """
    queues = []
    try:
        queue_exclude = [i.strip() for i in cp.get("lsf", "queue_exclude").\
            split(',')]
    except:
        queue_exclude = []
    rvf_info = parseRvf('lsf.rvf')
    rvf_queue_list = rvf_info.get('queue', {}).get('Values', None)
    if rvf_queue_list:
        rvf_queue_list = rvf_queue_list.split()
        log.info("The RVF lists the following queues: %s." % ', '.join( \
            rvf_queue_list))
    for queue in getQueueInfo(cp):
        if queue not in queue_exclude:
            queues.append(queue)
        if rvf_queue_list and queue not in rvf_queue_list:
            continue
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
    rvf_info = parseRvf('lsf.rvf')
    rvf_queue_list = rvf_info.get('queue', {}).get('Values', None)
    if rvf_queue_list:
        rvf_queue_list = rvf_queue_list.split()
        log.info("The RVF lists the following queues: %s." % ', '.join( \
            rvf_queue_list))
    vo_queues= []
    for queue in queueInfo:
        if queue in queue_exclude:
            continue
        if rvf_queue_list and queue not in rvf_queue_list:
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

