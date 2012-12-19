
"""
Module for interacting with SLURM.
"""

import re
import grp
import pwd
import gip_sets as sets
import os

from gip_common import HMSToMin, getLogger, VoMapper, voList, parseRvf
from gip_common import addToPath, cp_get
from gip_testing import runCommand

log = getLogger("GIP.SLURM")

# TODO: replace with slurm equivalents
batch_system_info_cmd = "sinfo -V"
jobs_cmd = 'squeue -h -o "%i %u %T %P"'
queue_info_cmd = 'sinfo -h -o "%P %a %C %l"'
#queue_info_cmd = "qstat -Q -f %(slurmHost)s"
#jobs_cmd = "qstat"
#slurmnodes_cmd = "slurmnodes -a"

def slurmOutputFilter(fp):
    return fp

def slurmCommand(command, cp):
    """
    Run a command against the SLURM batch system.
    """

    slurm_path = cp_get(cp, "slurm", "slurm_path", ".")
    addToPath(slurm_path)
    addToPath(slurm_path + "/bin")
    fp = runCommand(command)

    return slurmOutputFilter(fp)

def getLrmsInfo(cp):
    """
    Return the version string from the SLURM batch system.
    """
    version_re = re.compile("slurm (.*)\n")
    for line in slurmCommand(batch_system_info_cmd, cp):
        m = version_re.search(line)
        if m:
            return m.groups()[0]
    raise Exception("Unable to determine LRMS version info.")

def getJobsInfo(vo_map, cp):
    """
    Return information about the jobs currently running in SLURM
    We treat SLURM partitions as queues.
    
    The return value is a dictionary of dictionaries; the keys for the
    top-level dictionary are queue names; the values are queuedata dictionaries
    
    The queuedata dicts have key:val pairs of voname: voinfo, where voinfo is
    a dictionary with the following keys:
       - running: Number of VO running jobs in this queue.
       - wait: Number of VO waiting jobs in this queue.
       - total: Number of VO total jobs in this queue.
    
    @param vo_map: A VoMapper object which is used to map user names to VOs.
    @param cp: Site configuration object
    @return: A dictionary containing queue job information.
    """
    queue_jobs = {}
    for orig_line in slurmCommand(jobs_cmd, cp):
        # START HERE
        try:
            job, user, status, queue = orig_line.split()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            continue

        queue_data = queue_jobs.get(queue, {})
        try:
            vo = vo_map[user].lower()
        except:
            # Most likely, this means that the user is local and not
            # associated with a VO, so we skip the job.
            continue
        info = queue_data.get(vo, {"running":0, "wait":0, "total":0})
        if status == "RUNNING" or status == "COMPLETING":
            info["running"] += 1
        if status == "PENDING" or status == "CONFIGURING":
            info["wait"] += 1
        info["total"] += 1
        queue_data[vo] = info
        queue_jobs[queue] = queue_data
    return queue_jobs

def getQueueInfo(cp):
    """
    Looks up the queue information from SLURM.

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
    for orig_line in slurmCommand(queue_info_cmd, cp):
        queue_data = {}
        queue_name, status, nodestatus, timelimit = orig_line.split()
        active, idle, other, total = nodestatus.split('/')

        if status == 'up':
            queue_data["status"] = "Production"
        else:
            queue_data["status"] = "Closed"

        # should we subtract out "other"?
        queue_data['job_slots'] = int(total)
        queue_data['max_running'] = int(total)
        queue_data['free_slots'] = int(idle)

        # SLURM priorities depend on the scheduler, I think -- for backfill and FIFO, setting
        #  node weights and/or partition priorities didn't make any scheduling difference.
        queue_data['priority'] = 0.0

        # 1 year is close enough to infinity in the grid world..
        queue_data['max_wall'] = 1440*365
        if timelimit != "infinite":
            queue_data['max_wall'] = slurmTimeToMinutes(timelimit)

        # start from zero:
        queue_data['running'] = 0
        queue_data['wait'] = 0
        queue_data['total'] = 0

        queueInfo[queue_name] = queue_data

    for orig_line in slurmCommand(jobs_cmd, cp):
        try:
            job, user, status, queue_name = orig_line.split()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            continue

        queue_data = queueInfo[queue_name]
        queue_data['running'] = queue_data.get('running', 0)
        queue_data['wait'] = queue_data.get('wait', 0)
        queue_data['total'] = queue_data.get('total', 0)

        if status == "RUNNING" or status == "COMPLETING":
            queue_data["running"] += 1
        if status == "PENDING" or status == "CONFIGURING":
            queue_data["wait"] += 1
        queue_data["total"] += 1

    return queueInfo

def slurmTimeToMinutes(slurm_time):
    # Convert SLURM time to seconds

    # slurm_time format can be:
    #      days-hours:minutes:seconds
    #           hours:minutes:seconds
    #                 minutes:seconds
    timeArray = slurm_time.split(":")
    seconds = int(timeArray.pop())
    minutes = int(timeArray.pop())
    days, hours = (0,0)

    if timeArray:
        daysHours = timeArray[0].split("-")
        hours = int(daysHours.pop())
        if daysHours:
            days = int(daysHours.pop())

    return int(days)*1440 + int(hours)*60 + int(minutes) + int(round(int(seconds)/60.0))

def parseNodes(cp, version):
    """
    Parse the node information from SLURM.  Using the output from slurmnodes, 
    determine:
    
        - The number of total CPUs in the system.
        - The number of free CPUs in the system.
        - A dictionary mapping SLURM queue names to a tuple containing the
            (totalCPUs, freeCPUs).
    """
    totalCpu = 0
    freeCpu = 0
    queueCpu = {}
    queue = None
    avail_cpus = None
    used_cpus = None
    if version.find("SLURMPro") >= 0:
        for line in slurmCommand(slurmnodes_cmd, cp):
            if len(line.strip()) == 0:
                continue
            if not line.startswith('    ') and avail_cpus != None:
                if queue != None:
                    info = queueCpu.get(queue, [0, 0])
                    info[0] += avail_cpus
                    info[1] += avail_cpus - used_cpus
                    queueCpu[queue] = info
                else:
                    totalCpu += avail_cpus
                    freeCpu += avail_cpus - used_cpus
                queue = None
                continue
            line = line.strip()
            try:
                attr, val = line.split(" = ")
            except:
                continue
            if attr == "resources_available.ncpus":
                avail_cpus = int(val)
            elif attr == "resources_assigned.ncpus":
                used_cpus = int(val)
    else:
        for line in slurmCommand(slurmnodes_cmd, cp):
            try:
                attr, val = line.split(" = ")
            except:
                continue
            val = val.strip()
            attr = attr.strip()
            if attr == "state":
                state = val
            if attr == "np":
                try:
                    np = int(val)
                except:
                    np = 1
                if not (state.find("down") >= 0 or \
                        state.find("offline") >= 0):
                    totalCpu += np
                if state.find("free") >= 0:
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
        queue_exclude = [i.strip() for i in cp.get("slurm", "queue_exclude").\
            split(',')]
    except:         
        queue_exclude = []
    rvf_info = parseRvf('slurm.rvf')
    rvf_queue_list = rvf_info.get('queue', {}).get('Values', None)
    if rvf_queue_list: 
        rvf_queue_list = rvf_queue_list.split()
        log.info("The RVF lists the following queues: %s." % ', '.join( \
            rvf_queue_list))
    for queue in getQueueInfo(cp):
        if rvf_queue_list and queue not in rvf_queue_list:
            continue
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
        queue_exclude = [i.strip() for i in cp.get("slurm", "queue_exclude").\
            split(',')]
    except:
        queue_exclude = []
    vo_queues = []
    queueInfo = getQueueInfo(cp)
    rvf_info = parseRvf('slurm.rvf')
    rvf_queue_list = rvf_info.get('queue', {}).get('Values', None)
    if rvf_queue_list:
        rvf_queue_list = rvf_queue_list.split()
        log.info("The RVF lists the following queues: %s." % ', '.join( \
            rvf_queue_list))
    for queue, qinfo in queueInfo.items():
        if rvf_queue_list and queue not in rvf_queue_list:
            continue
        if queue in queue_exclude:
            continue
        volist = sets.Set(voList(cp, voMap))
        try:
            whitelist = [i.strip() for i in cp.get("slurm", "%s_whitelist" % \
                queue).split(',')]
        except:
            whitelist = []
        whitelist = sets.Set(whitelist)
        try:
            blacklist = [i.strip() for i in cp.get("slurm", "%s_blacklist" % \
                queue).split(',')]
        except:
            blacklist = []
        blacklist = sets.Set(blacklist)
        if 'users' in qinfo or 'groups' in qinfo:
            acl_vos = parseAclInfo(queue, qinfo, voMap)
            volist.intersection_update(acl_vos)
        # Force any VO in the whitelist to show up in the volist, even if it
        # isn't in the acl_users / acl_groups
        for vo in whitelist:
            if vo not in volist:
                volist.add(vo)
        # Apply white and black lists
        for vo in volist:
            if (vo in blacklist or "*" in blacklist) and ((len(whitelist) == 0)\
                    or vo not in whitelist):
                continue
            vo_queues.append((vo, queue))
    return vo_queues

def parseAclInfo(queue, qinfo, vo_mapper):
    """
    Take a queue information dictionary and determine which VOs are in the ACL
    list.  The used keys are:

       - users: A set of all user names allowed to access this queue.
       - groups: A set of all group names allowed to access this queue.

    @param queue: Queue name (for logging purposes).
    @param qinfo: Queue info dictionary
    @param vo_mapper: VO mapper object
    @returns: A set of allowed VOs
    """
    users = qinfo.get('users', sets.Set())
    if 'groups' in qinfo:
        all_groups = grp.getgrall()
        all_users = pwd.getpwall()
        group_dict = {}
        for group in all_groups:
            if group[0] in qinfo['groups'] or group[2] in qinfo['groups']:
                users.add(group[0])
            group_dict[group[2]] = group[0]
        for user in all_users:
            try:
                group = group_dict[user[3]]
            except:
                continue
            if group[0] in qinfo['groups'] or user[3] in qinfo['groups']:
                users.add(group[0])
    vos = sets.Set()
    for user in users:
        try:
            vos.add(vo_mapper[user])
        except:
            pass
    log.info("The acl info for queue %s (users %s, groups %s) mapped to %s." % \
        (queue, ', '.join(qinfo.get('users', [])),
        ', '.join(qinfo.get('groups', [])), ', '.join(vos)))
    return vos

