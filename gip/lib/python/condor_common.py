
from gip_common import runCommand, voList

condor_version = "condor_version"
condor_group = "condor_config_val GROUP_NAMES"
condor_quota = "condor_config_val GROUP_QUOTA_group_%(group)s"
condor_prio = "condor_config_val GROUP_PRIO_FACTOR_group_%(group)s"
condor_status = "condor_status -pool %(central_manager)s"

def condorCommand(command, cp, info={}):
    cmd = command % info
    return runCommand(cmd)

def getLrmsInfo(cp):
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

def getCentralManager(cp):
    manager = condorCommand(condor_manager, cp).split(',')[0].strip()
    return manager

def getJobsInfo(vo_map, cp):
    fp = condorCommand(condor_q, cp, {'manager': getCentralManager(cp)})
    info = fp.read()
    info = info[info.find('\n', 3)+1:]
    retval = {}
    groupsInfo = getGroupInfo(vo_map, cp)
    for ctag in parseString.getElementsByTagName('c'):
        owner = None
        status = None
        for tag in parseString.getElementsByTagName('a'):
            if tag.getAttribute('n') == 'Owner':
                owner = tag.firstChild.firstChild.data
                vo = vo_map[owner]
                if vo not in retval:
                    retval[vo] = {'running': 0, 'queued': 0}
            if tag.getAttribute('n') == 'JobStatus':
                status = int(tag.firstChild.firstChild.data)
        if owner == None or status==None:
            raise ValueError("Invalid condor job!")
        if status == 2:
            retval[vo]['running'] += 1
        else:
            retval[vo]['queued'] += 1
    for vo in retval:
        if vo in groupsInfo:
            retval[vo]['quota'] = groupsInfo['quota']
            retval[vo]['prio'] = groupsInfo['prio']
        else:
            retval[vo]['quota'] = 0
            retval[vo]['prio'] = 0
    return retval

def parseNodes(cp):
    manager = getCentralManager(cp)
    info = {'manager': manager}
    at_totals = False
    for line in condorCommand(condor_status, cp, info):
        if line.find("Total Owner Claimed Unclaimed Matched Preempting " \
                "Backfill") >= 0:
            at_totals = True
            continue
        if at_totals and line.find("Total") >= 0:
            info = line.split()
            total, owner, claimed, unclaimed = total[1:5]
            break
    if cp.getboolean("condor", "subtract_owner"):
        total -= owner
    return total, claimed, unclaimed

