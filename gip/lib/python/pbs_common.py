
import re, sys, os
from gip_common import HMSToMin, getLogger, runCommand

log = getLogger("GIP.PBS")

batch_system_info_cmd = "qstat -B -f %(pbsHost)s"
queue_info_cmd = "qstat -Q -f %(pbsHost)s"
jobs_cmd = "qstat"
pbsnodes_cmd = "pbsnodes -a"

def pbsOutputFilter(fp):
    """
    PBS can be a pain to work with because it automatically cuts 
    lines off at 80 chars and continues the line on the next line.  For
    example:

    Server: red
    server_state = Active
    server_host = red.unl.edu
    scheduling = True
    total_jobs = 2996
    state_count = Transit:0 Queued:2568 Held:0 Waiting:0 Running:428 Exiting:0 
        Begun:0 
    acl_roots = t3
    managers = mfurukaw@red.unl.edu,root@t3

    This function puts the line "Begun:0" with the above line.  It's meant
    to filter the output, so you should "scrub" PBS output like this:
    fp = runCommand(<pbs command>)
    for line in pbsOutputFilter(fp):
       ... parse line ...

    This function uses iterators
    """
    class PBSIter:

        def __init__(self, fp):
            self.fp = fp
            self.fp_iter = fp.__iter__()
            self.prevline = None
            self.done = False

        def next(self):
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

        def __init__(self, iter):
            self.iter = iter

        def __iter__(self):
            return self.iter

    return PBSFilter(PBSIter(fp))

def pbsCommand(command, cp):
    pbsHost = cp.get("pbs", "host")
    if pbsHost.lower() == "none" or pbsHost.lower() == "localhost":
        pbsHost = ""
    cmd = command % {'pbsHost': pbsHost}
    fp = runCommand(cmd)
    #pid, exitcode = os.wait()
    #if exitcode != 0:
    #    raise Exception("Command failed: %s" % cmd)
    return pbsOutputFilter(fp)

def getLrmsInfo(cp):
    version_re = re.compile("pbs_version = (.*)\n")
    for line in pbsCommand(batch_system_info_cmd, cp):
        m = version_re.search(line)
        if m:
            return m.groups()[0]
    raise Exception("Unable to determine LRMS version info.")

def getJobsInfo(vo_map, cp):
    queue_jobs = {}
    for orig_line in pbsCommand(jobs_cmd, cp):
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
    
       * The number of total CPUs in the system.
       * The number of free CPUs in the system.
       * A dictionary mapping PBS queue names to a tuple containing the
         (totalCPUs, freeCPUs).
    """
    totalCpu = 0
    freeCpu = 0
    queueCpu = {}
    queue = None
    avail_cpus = None
    used_cpus = None
    if version.find("PBSPro") >= -1:
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


