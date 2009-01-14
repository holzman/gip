
"""
Module for interacting with PBS.
"""

import re
import grp
import pwd
import gip_sets as sets

from gip_common import HMSToMin, getLogger, VoMapper, voList, parseRvf
from gip_testing import runCommand

log = getLogger("GIP.PBS")

batch_system_info_cmd = "qstat -B -f %(pbsHost)s"
queue_info_cmd = "qstat -Q -f %(pbsHost)s"
jobs_cmd = "qstat"
pbsnodes_cmd = "pbsnodes -a"

def pbsOutputFilter(fp):
    """
    PBS can be a pain to work with because it automatically cuts 
    lines off at 80 chars and continues the line on the next line.  For
    example::

        Server: red
        server_state = Active
        server_host = red.unl.edu
        scheduling = True
        total_jobs = 2996
        state_count = Transit:0 Queued:2568 Held:0 Waiting:0 Running:428 Exiting 
         :0 Begun:0 
        acl_roots = t3
        managers = mfurukaw@red.unl.edu,root@t3

    This function puts the line ":0 Begun:0" with the above line.  It's meant
    to filter the output, so you should "scrub" PBS output like this::

        fp = runCommand(<pbs command>)
        for line in pbsOutputFilter(fp):
           ... parse line ...

    This function uses iterators
    """
    class PBSIter:
        """
        An iterator for PBS output; this allows us to easily parse over 
        PBS-style line continuations.
        """

        def __init__(self, fp):
            self.fp = fp
            self.fp_iter = fp.__iter__()
            self.prevline = None
            self.done = False

        def next(self):
            """
            Return the next full line of output for the iterator.
            """
            if self.prevline == None:
                line = self.fp_iter.next()
                if line.startswith('\t'):
                    # Bad! The output shouldn't start with a 
                    # partial line
                    raise ValueError("PBS output contained bad data.")
                self.prevline = line
                return self.next()
            if self.done:
                raise StopIteration()
            try:
                line = self.fp_iter.next()
                if line.startswith('\t'):
                    self.prevline = self.prevline[:-1] + line[1:-1]
                    return self.next()
                else:
                    old_line = self.prevline
                    self.prevline = line
                    return old_line
            except StopIteration:
                self.done = True
                return self.prevline

    class PBSFilter:
        """
        An iterable object based upon the PBSIter iterator.
        """
        
        def __init__(self, myiter):
            self.iter = myiter

        def __iter__(self):
            return self.iter

    return PBSFilter(PBSIter(fp))

def pbsCommand(command, cp):
    """
    Run a command against the PBS batch system.
    
    Use this when talking to PBS; not only does it allow for integration into
    the GIP test framework, but it also filters and expands PBS-style line
    continuations.
    """
    try:
        pbsHost = cp.get("pbs", "host")
    except:
        pbsHost = ""
    if pbsHost.lower() == "none" or pbsHost.lower() == "localhost":
        pbsHost = ""
    cmd = command % {'pbsHost': pbsHost}
    fp = runCommand(cmd)
    #pid, exitcode = os.wait()
    #if exitcode != 0:
    #    raise Exception("Command failed: %s" % cmd)
    return pbsOutputFilter(fp)

def getLrmsInfo(cp):
    """
    Return the version string from the PBS batch system.
    """
    version_re = re.compile("pbs_version = (.*)\n")
    for line in pbsCommand(batch_system_info_cmd, cp):
        m = version_re.search(line)
        if m:
            return m.groups()[0]
    raise Exception("Unable to determine LRMS version info.")

def getJobsInfo(vo_map, cp):
    """
    Return information about the jobs currently running in PBS
    
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
    for orig_line in pbsCommand(jobs_cmd, cp):
        try:
            job, _, user, _, status, queue = orig_line.split()
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
    Looks up the queue information from PBS.

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
    for orig_line in pbsCommand(queue_info_cmd, cp):
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
        elif attr == "max_queuable" or attr == 'max_queueable':
            try:
                queue_data["max_waiting"] = int(val)
                queue_data["max_queuable"] = int(val)
            except: 
                log.warning("Invalid input for max_queuable: %s" % str(val))
        elif attr == "acl_group_enable" and val.lower() == 'true':
            queue_data["groups"] = sets.Set()
        elif attr == "acl_groups" and 'groups' in queue_data:
            queue_data["groups"].update(val.split(','))
        elif attr == "acl_user_enable" and val.lower() == 'true':
            queue_data["users"] = sets.Set()
        elif attr == "acl_users" and 'users' in queue_data:
            queue_data["users"].update(val.split(','))
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
    Parse the node information from PBS.  Using the output from pbsnodes, 
    determine:
    
        - The number of total CPUs in the system.
        - The number of free CPUs in the system.
        - A dictionary mapping PBS queue names to a tuple containing the
            (totalCPUs, freeCPUs).
    """
    totalCpu = 0
    freeCpu = 0
    queueCpu = {}
    queue = None
    avail_cpus = None
    used_cpus = None
    if version.find("PBSPro") >= 0:
        for line in pbsCommand(pbsnodes_cmd, cp):
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
        for line in pbsCommand(pbsnodes_cmd, cp):
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
        queue_exclude = [i.strip() for i in cp.get("pbs", "queue_exclude").\
            split(',')]
    except:         
        queue_exclude = []
    rvf_info = parseRvf('pbs.rvf')
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
        queue_exclude = [i.strip() for i in cp.get("pbs", "queue_exclude").\
            split(',')]
    except:
        queue_exclude = []
    vo_queues = []
    queueInfo = getQueueInfo(cp)
    rvf_info = parseRvf('pbs.rvf')
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
            whitelist = [i.strip() for i in cp.get("pbs", "%s_whitelist" % \
                queue).split(',')]
        except:
            whitelist = []
        whitelist = sets.Set(whitelist)
        try:
            blacklist = [i.strip() for i in cp.get("pbs", "%s_blacklist" % \
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

