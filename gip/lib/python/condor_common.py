
"""
Common function which provide information about the Condor batch system.

This module interacts with condor through the following commands:
  - condor_q
  - condor_status
"""
from gip_common import voList, cp_getBoolean
from gip_testing import runCommand

condor_version = "condor_version"
condor_group = "condor_config_val GROUP_NAMES"
condor_quota = "condor_config_val GROUP_QUOTA_group_%(group)s"
condor_prio = "condor_config_val GROUP_PRIO_FACTOR_group_%(group)s"
condor_status = "condor_status"
condor_job_status = "condor_status -submitter -format '%s:' Name -format '%d:' RunningJobs -format '%d:' IdleJobs -format '%d\n' HeldJobs"

def condorCommand(command, cp, info={}):
    """
    Execute a command in the shell.  Returns a file-like object
    containing the stdout of the command

    Use this function instead of executing directly (os.popen); this will
    allow you to hook your providers into the testing framework.
    """

    cmd = command % info
    return runCommand(cmd)

def getLrmsInfo(cp):
    """
    Get information from the LRMS (batch system).

    Returns the version of the condor client on your system.
    """
    for line in condorCommand(condor_version, cp):
        if line.startswith("$CondorVersion:"):
            return line[15:].strip()
    raise ValueError("Bad output from condor_version.")

def getGroupInfo(vo_map, cp):
    output = condorCommand(condor_group, cp)
    if line.startswith("Not defined"):
        return {}
    retval = {}
    for group in output.split(','):
        group = group.strip()
        quota = condorCommand(condor_quota, cp, {'group': group}).strip()
        prio = condorCommand(condor_prio, cp, {'group': group}).strip()
        vo = vo_map[group]
        retval[vo] = {'quota': quota, 'prio': prio}
    return retval

#def getCentralManager(cp):
#    manager = condorCommand(condor_manager, cp).split(',')[0].strip()
#    return manager

def getJobsInfo(vo_map, cp):

    # Job stats
    # Held jobs are included as "waiting" since the definition is:
    #    Number of jobs that are in a state different than running

    vo_jobs = {}
    fp = condorCommand(condor_job_status, cp)
    jobs = fp.read().split("\n")
    for line in jobs:
        name, running, idle, held = line.split(":")
        name = name.split("@")[0]

        try:
            vo = vo_map[user].lower()
        except:
            # Most likely, this means that the user is local and not
            # associated with a VO, so we skip the job.
            continue

        info = vo_jobs.get(vo, {"running":0, "idle":0, "held":0})
        info["running"] += 1
        info["idle"] += 1
        info["held"] += 1

        vo_jobs[vo] = info
    return vo_jobs

def parseNodes(cp):
    total, owner, claimed, unclaimed, Matched, Preempting, Backfill = 0
    at_totals = False
    for line in condorCommand(condor_status, cp):
        if line.find("Total Owner Claimed Unclaimed Matched Preempting Backfill") >= 0:
            at_totals = True
            continue
        if at_totals and line.find("Total") >= 0:
            info = line.split()
            total, owner, claimed, unclaimed, Matched, Preempting, Backfill = total[1:]
            total -= owner
            break
    if not cp_getBoolean("condor", "subtract_owner"):
        total += owner
    return total, claimed, unclaimed

