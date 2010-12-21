#!/usr/bin/python

"""
Provide the information related to the Condor batch system.  The general
outline of how this is computed is given here:

https://twiki.grid.iu.edu/twiki/bin/view/InformationServices/GipCeInfo
"""

import gip_sets as sets
import sys
py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3
if not py23:
        import operator
        def sum(data, start=0): return reduce(operator.add, data, start)

# Standard GIP imports
import gip_cluster
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate, \
    voList, printTemplate, cp_get, cp_getBoolean, cp_getInt, responseTimes
from gip_cluster import getClusterID
from condor_common import parseNodes, getJobsInfo, getLrmsInfo, getGroupInfo
from condor_common import defaultGroupIsExcluded
from gip_storage import getDefaultSE
from gip_batch import buildCEUniqueID, getGramVersion, getCEImpl, getPort, \
     buildContactString

from gip_sections import ce, se

log = getLogger("GIP.Condor")

def print_CE(cp):
    """
    Print out the CE(s) for Condor

    Config options used:
       * ce.name.  The name of the CE.  Defaults to "".
       * condor.status.  The status of the condor LRMS.  Defaults to
          "Production".
       * ce.globus_version.  The used Globus version.  Defaults to 4.0.6
       * ce.hosting_cluster.  The attached cluster name.  Defaults to ce.name
       * ce.host_name.  The CE's host name.  Default to ce.name
       * condor.preemption.  Whether or not condor allows preemption.  Defaults
          to False
       * condor.max_wall.  The maximum allowed wall time for Condor.  Defaults
          to 1440 (in minutes)
       * osg_dirs.app.  The $OSG_APP directory.  Defaults to "/Unknown".
       * osg_dirs.data.  The $OSG_DATA directory.  Defaults to "/Unknown".
       * se.name.  The human-readable name of the closest SE
       * bdii.endpoint.  The endpoint of the BDII this will show up in.

    @param cp: The GIP configuration
    @type cp: ConfigParser.ConfigParser
    """
    ce_template = getTemplate("GlueCE", "GlueCEUniqueID")
    ce_name = cp_get(cp, "ce", "name", "UNKNOWN_CE")

    status = cp_get(cp, "condor", "status", "Production")
    
    # Get condor version
    try:
        condorVersion = getLrmsInfo(cp)
    except:
        condorVersion = "Unknown"

    # Get the node information for condor
    try:
        total_nodes, claimed, unclaimed = parseNodes(cp)
    except Exception, e:
        log.exception(e)
        total_nodes, claimed, unclaimed = 0, 0, 0

    vo_map = VoMapper(cp)

    # Determine the information about the current jobs in queue
    try:
        jobs_info = getJobsInfo(vo_map, cp)
    except Exception, e:
        log.exception(e)
        jobs_info = {'default': dict([(vo, {'running': 0, 'idle': 0,
            'held': 0}) for vo in voList(cp)])}

    # Determine the group information, if there are any Condor groups
    try:
        groupInfo = getGroupInfo(vo_map, cp)
    except Exception, e:
        log.exception(e)
        # Default to no groups.
        groupInfo = {}

    log.debug("Group Info: %s" % str(groupInfo))

    # Accumulate the entire statistics, instead of breaking down by VO.
    running, idle, held = 0, 0, 0
    for group, ginfo in jobs_info.items():
        for vo, info in ginfo.items():
            running += info.get('running', 0)
            idle += info.get('idle', 0)
            held += info.get('held')

    # Set up the "default" group with all the VOs which aren't already in a 
    # group
    groupInfo['default'] = {'prio': 999999, 'quota': 999999, 'vos': sets.Set()}
    all_group_vos = []
    total_assigned = 0
    for key, val in groupInfo.items():
        if key == 'default':
            continue
        all_group_vos.extend(val['vos'])
        try:
            total_assigned += val['quota']
        except:
            pass
    if total_nodes > total_assigned:
        log.info("There are %i assigned job slots out of %i total; assigning" \
            " the rest to the default group." % (total_assigned, total_nodes))
        groupInfo['default']['quota'] = total_nodes-total_assigned
    else:
        log.warning("More assigned nodes (%i) than actual nodes (%i)! Assigning" \
	   " all slots also to the default group." % (total_assigned, total_nodes))
	# NB: If you sum up the GlueCEInfoTotalCPUs for every queue, you will calculate
	# more slots than the batch system actually has.  That seems fair, since the group
	# quotas are oversubscribed.
	groupInfo['default']['quota'] = 0  # will get transformed to total_nodes below
	
    all_vos = voList(cp)
    defaultVoList = [i for i in all_vos if i not in all_group_vos]
    groupInfo['default']['vos'] = defaultVoList
    #acbr = '\n'.join(['GlueCEAccessControlBaseRule: VO:%s' % i for i in \
    #    defaultVoList])
    #groupInfo['default']['acbr'] = acbr
    if not groupInfo['default']['vos'] or defaultGroupIsExcluded(cp):
        if groupInfo.has_key('default'):
            del groupInfo['default']
        
    for group, ginfo in groupInfo.items():
        jinfo = jobs_info.get(group, {})
	ce_unique_id = buildCEUniqueID(cp, ce_name, 'condor', group)	
        vos = ginfo['vos']
        if not isinstance(vos, sets.Set):
            vos = sets.Set(vos)
        vos.update(jinfo.keys())
        vos.intersection_update(sets.Set(all_vos))
        log.debug("CE %s, post-filtering VOs: %s." % (ce_unique_id, ", ".join(\
            vos)))
        if not vos:
            continue
        if 'acbr' not in ginfo:
            ginfo['acbr'] = '\n'.join(['GlueCEAccessControlBaseRule: VO:%s' % \
                vo for vo in vos])
        max_running = 0
        if group in jobs_info:
            max_runnings = [i.get('max_running', 0) for i in \
                            jobs_info[group].values()]
            if max_runnings:
                max_running = max(max_runnings)
        if ginfo.get('quota', 0) != 999999:
            max_running = max(max_running, ginfo.get('quota', 0))

        # Invariants:
        # If (ASSIGNED is defined): TOTAL <= ASSIGNED
        if ginfo.get("quota", 0) > 0:
            # Force the total nodes to be at most the maximum running
            ce_total_nodes = min(ginfo.get("quota", 0), total_nodes)
            # The assigned job slots is equal to the quota.
            # The total nodes may not be greater than the quota.
            # The assigned slots may be larger than the total.
            assigned = ginfo.get("quota", 0)
        else:
            # Default to having the assigned jobs = total nodes.
            assigned = total_nodes
            ce_total_nodes = total_nodes

        myrunning = sum([i.get('running', 0) for i in jinfo.values()], 0)

        # If the running jobs are greater than the total/assigned, bump
        # up the values of the total/assigned
        # Keeps the invariant: RUNNING <= ASSIGNED, RUNNING <= TOTAL
        assigned = max(assigned, myrunning)
        ce_total_nodes = max(assigned, ce_total_nodes)

        # Make sure the following holds:
        # CE_FREE_SLOTS <= ASSIGNED - RUNNING
        # CE_FREE_SLOTS <= UNCLAIMED SLOTS IN CONDOR
        ce_unclaimed = min(assigned - myrunning, unclaimed)

        myidle = sum([i.get('idle', 0) for i in jinfo.values()], 0)
        myheld = sum([i.get('held', 0) for i in jinfo.values()], 0)

        max_wall = cp_getInt(cp, "condor", "max_wall", 1440)
        ert, wrt = responseTimes(cp, myrunning, myidle+myheld,
            max_job_time=max_wall*60)

        referenceSI00 = gip_cluster.getReferenceSI00(cp)

	contact_string = buildContactString(cp, 'condor', group, ce_unique_id, log)
		
        extraCapabilities = ''
	if cp_getBoolean(cp, 'site', 'glexec_enabled', False):
	    extraCapabilities = extraCapabilities + '\n' + 'GlueCECapability: glexec'

	gramVersion = getGramVersion(cp)
	ceImpl, ceImplVersion = getCEImpl(cp)
	port = getPort(cp)

        # Build all the GLUE CE entity information.
        info = { \
            "ceUniqueID"     : ce_unique_id,
            'contact_string' : contact_string,
            "ceImpl"         : ceImpl,
            "ceImplVersion"  : ceImplVersion,
            "hostingCluster" : cp_get(cp, ce, 'hosting_cluster', ce_name),
            "hostName"       : cp_get(cp, ce, "host_name", ce_name),
            "gramVersion"    : gramVersion,
            "lrmsType"       : "condor",
            "port"           : port,
            "running"        : myrunning,
            "idle"           : myidle,
            "held"           : myheld,
            "ce_name"        : ce_name,
            "ert"            : ert,
            "wrt"            : wrt,
            "job_manager"    : 'condor',
            "queue"          : group,
            "lrmsVersion"    : condorVersion,
            "job_slots"      : int(ce_total_nodes),
            "free_slots"     : int(ce_unclaimed),
            # Held jobs are included as "waiting" since the definition is:
            #    Number of jobs that are in a state different than running
            "waiting"        : myidle + myheld,
            "running"        : myrunning,
            "total"          : myrunning + myidle + myheld,
            "priority"       : ginfo.get('prio', 0),
            "assigned"       : assigned,
            "max_slots"      : 1,
            "preemption"     : str(int(cp_getBoolean(cp, "condor", \
                "preemption", False))),
            "max_running"    : max_running,
            "max_waiting"    : 99999,
            "max_total"      : 99999,
            "max_wall"       : cp_getInt(cp, "condor", "max_wall", 1440),
            "status"         : status,
            'app_dir'        : cp_get(cp, 'osg_dirs', 'app', '/Unknown'),
            "data_dir"       : cp_get(cp, "osg_dirs", "data", "/Unknown"),
            "default_se"     : getDefaultSE(cp),
            "acbr"           : ginfo['acbr'],
            "referenceSI00"  : referenceSI00,
            "clusterUniqueID": getClusterID(cp),
            "bdii"           : cp_get(cp, "bdii", "endpoint", "Unknown"),
	    'extraCapabilities' : extraCapabilities
        }
        printTemplate(ce_template, info)
    return total_nodes, claimed, unclaimed

def print_VOViewLocal(cp):
    """
    Print the GLUE VOView entity; shows the VO's view of the condor batch
    system.

    Config options used:
        * ce.name.  The human-readable name of the ce.
        * condor.status.  The status of condor; defaults to "Production"
        * osg_dirs.app.  The $OSG_APP directory; defaults to "/Unknown"
        * osg_dirs.data.  The $OSG_DATA directory; defaults to "/Unknown"
        * se.name. The human-readable name of the closest SE.

    @param cp:  The GIP configuration object
    @type cp: ConfigParser.ConfigParser
    """
    VOView = getTemplate("GlueCE", "GlueVOViewLocalID")
    ce_name = cp_get(cp, "ce", "name", "")
    
    #status = cp_get(cp, "condor", "status", "Production")
    #condorVersion = getLrmsInfo(cp) 
    total_nodes, _, unclaimed = parseNodes(cp)
    
    vo_map = VoMapper(cp)
    jobs_info = getJobsInfo(vo_map, cp)
    groupInfo = getGroupInfo(vo_map, cp)

    # Add in the default group
    all_group_vos = []    
    total_assigned = 0
    for key, val in groupInfo.items():
        if key == 'default':
            continue
        all_group_vos.extend(val['vos'])
        total_assigned += val.get('quota', 0)
    all_vos = sets.Set(voList(cp))
    defaultVoList = [i for i in all_vos if i not in all_group_vos]
    if 'default' not in groupInfo:
        groupInfo['default'] = {}
    groupInfo['default']['vos'] = defaultVoList

    if total_nodes > total_assigned:
        log.info("There are %i assigned job slots out of %i total; assigning" \
            " the rest to the default group." % (total_assigned, total_nodes))
        groupInfo['default']['quota'] = total_nodes-total_assigned
    else:
        log.warning("More assigned nodes (%i) than actual nodes (%i)!" % \
            (total_assigned, total_nodes))

    if defaultGroupIsExcluded(cp):
        if groupInfo.has_key('default'):
            del groupInfo['default']
        
    for group in groupInfo:
        jinfo = jobs_info.get(group, {})
        vos = sets.Set(groupInfo[group].get('vos', [group]))
        vos.update(jinfo.keys())
        vos.intersection_update(all_vos)

        # Enforce invariants
        # VO_FREE_SLOTS <= CE_FREE_SLOTS
        # VO_FREE_SLOTS <= CE_ASSIGNED - VO_RUNNING
        # This code determines CE_ASSIGNED
        ginfo = groupInfo[group]
        if ginfo.get("quota", 0) > 0:
            assigned = ginfo.get("quota", 0)
        else:
            assigned = total_nodes

        log.debug("All VOs for %s: %s" % (group, ", ".join(vos)))
	ce_unique_id = buildCEUniqueID(cp, ce_name, 'condor', group)

        max_wall = cp_getInt(cp, "condor", "max_wall", 1440)

        myrunning = sum([i.get('running', 0) for i in jinfo.values()], 0)
        assigned = max(assigned, myrunning)
        
        for vo in vos:
            acbr = 'VO:%s' % vo
            info = jinfo.get(vo.lower(), {"running": 0, "idle": 0, "held": 0})
            ert, wrt = responseTimes(cp, info["running"], info["idle"] + \
                info["held"], max_job_time=max_wall*60)
            free = min(unclaimed, assigned-myrunning,
                assigned-int(info['running']))
            free = int(free)

            waiting = int(info["idle"]) + int(info["held"])
            if waiting > 0:
                free = 0

            info = {"vo"      : vo,
                "acbr"        : acbr,
                "ceUniqueID"  : ce_unique_id,
                "voLocalID"   : vo,
                "ce_name"     : ce_name,
                "job_manager" : 'condor',
                "queue"       : vo,
                "running"     : info["running"],
                # Held jobs are included as "waiting" since the definition is:
                #    Number of jobs that are in a state different than running
                "waiting"     : waiting,
                "total"       : info["running"] + info["idle"] + info["held"],
                "free_slots"  : free,
                "job_slots"   : int(total_nodes),
                "ert"         : ert,
                "wrt"         : wrt,
                "default_se"  : getDefaultSE(cp),
                'app'     : cp_get(cp, 'osg_dirs', 'app', '/Unknown'),
                "data"    : cp_get(cp, "osg_dirs", "data", "/Unknown"),
                }
            printTemplate(VOView, info)

def main():
    """
    Main wrapper for the Condor batch system GIP information.
    """
    try:
        cp = config()
        condor_path = cp_get(cp, "condor", "condor_path", None)
        if condor_path != None:
            addToPath(condor_path)
        #vo_map = VoMapper(cp)
        getLrmsInfo(cp) 
        print_CE(cp)
        print_VOViewLocal(cp)
    except Exception, e:
        log.exception(e)
        raise

if __name__ == '__main__':
    main()

