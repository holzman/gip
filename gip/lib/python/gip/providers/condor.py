#!/usr/bin/python

"""
Provide the information related to the Condor batch system.  The general
outline of how this is computed is given here:

https://twiki.grid.iu.edu/twiki/bin/view/InformationServices/GipCeInfo
"""

import os
import re
import sys
import unittest

# Standard GIP imports
from gip_common import config, VoMapper, getLogger, addToPath, getTemplate, \
    voList, printTemplate, cp_get, ldap_boolean, cp_getBoolean, cp_getInt
from gip_cluster import getClusterID
from condor_common import parseNodes, getJobsInfo, getLrmsInfo, getGroupInfo

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
    ce_name = cp_get(cp, "ce", "name", "")

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
        jobs_info = dict([(vo, {'running': 0, 'idle': 0, 'held': 0}) for vo in \
            voList(cp)])

    # Determine the group information, if there are any Condor groups
    try:
        groupInfo = getGroupInfo(vo_map, cp)
    except Exception, e:
        log.exception(e)
        # Default to no groups.
        groupInfo = {}

    # Accumulate the entire statistics, instead of breaking down by VO.
    running, idle, held = 0, 0, 0
    for vo, info in jobs_info.items():
        running += info.get('running', 0)
        idle += info.get('idle', 0)
        held += info.get('held')

    # Set up the "default" group with all the VOs which aren't already in a 
    # group
    groupInfo['default'] = {'prio': 0, 'quota': 0}
    defaultVoList = voList(cp)
    defaultVoList = [i for i in defaultVoList if i not in groupInfo]
    acbr = '\n'.join(['GlueAccessControlBaseRule: VO:%s' % i for i in \
        defaultVoList])
    groupInfo['default']['acbr'] = acbr

    for group, ginfo in groupInfo.items():
        ce_unique_id = '%s:2119/jobmanager-condor-%s' % (ce_name, group)
        if 'acbr' not in ginfo:
            ginfo['acbr'] = 'GlueAccessControlBaseRule: VO:%s' % group
        max_running = 0
        if group in jobs_info:
            max_running = jobs_info[group].get('max_running', 0)
        max_running = max(max_running, ginfo.get('quota', 0))
        assigned = max(ginfo.get("quota", 0), total_nodes)

        # Build all the GLUE CE entity information.
        info = { \
            "ceUniqueID"  : ce_unique_id,
            'contact_string': ce_unique_id,
            "ceImpl"      : "Globus",
            "ceImplVersion": cp_get(cp, ce, 'globus_version', '4.0.6'),
            "hostingCluster": cp_get(cp, ce, 'hosting_cluster', ce_name),
            "hostName"    : cp_get(cp, ce, "host_name", ce_name),
            "gramVersion" : '2.0',
            "lrmsType"    : "condor",
            "port"        : 2119,
            "running"     : running,
            "idle"        : idle,
            "held"        : held,
            "ce_name"     : ce_name,
            "ert"         : 3600,
            "wrt"         : 3600,
            "job_manager" : 'condor',
            "queue"       : group,
            "lrmsVersion" : condorVersion,
            "job_slots"   : int(total_nodes),
            "free_slots"  : int(unclaimed),
            # Held jobs are included as "waiting" since the definition is:
            #    Number of jobs that are in a state different than running
            "waiting"     : idle + held,
            "running"     : running,
            "total"       : running + idle + held,
            "priority"    : ginfo.get('prio', 0),
            "assigned"    : assigned,
            "max_slots"   : 1,
            "preemption"  : ldap_boolean(cp_getBoolean(cp, "condor", \
                "preemption", False)),
            "max_running" : max_running,
            "max_waiting" : 99999,
            "max_total"   : 99999,
            "max_wall"    : cp_getInt(cp, "condor", "max_wall", 1440),
            "status"      : status,
            'app_dir'     : cp_get(cp, 'osg_dirs', 'app', '/Unknown'),
            "data_dir"    : cp_get(cp, "osg_dirs", "data", "/Unknown"),
            "default_se"  : cp_get(cp, se, "name", "UNAVAILABLE"),
            "acbr"        : ginfo['acbr'],
            "clusterUniqueID": getClusterID(cp),
            "bdii"        : cp_get(cp, "bdii", "endpoint", "Unknown")
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
    
    status = cp_get(cp, "condor", "status", "Production")
    condorVersion = getLrmsInfo(cp) 
    total_nodes, claimed, unclaimed = parseNodes(cp)
    
    vo_map = VoMapper(cp)
    jobs_info = getJobsInfo(vo_map, cp)
    groupInfo = getGroupInfo(vo_map, cp)

    # Add in the default group
    defaultVoList = [i for i in voList(cp) if i not in groupInfo]
    groupInfo = {'default': {'vos': defaultVoList}}

    for group in groupInfo:
        vos = groupInfo[group].get('vos', [group])
        ce_unique_id = '%s:2119/jobmanager-condor-%s' % (ce_name, group)    
        for vo in vos:
            acbr = 'VO:%s' % vo
            info = jobs_info.get(vo, {"running": 0, "idle": 0, "held": 0})
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
                "waiting"     : info["idle"] + info["held"],
                "total"       : info["running"] + info["idle"] + info["held"],
                "free_slots"  : int(unclaimed),
                "job_slots"   : int(total_nodes),
                "ert"         : 3600,
                "wrt"         : 3600,
                "default_se"  : cp_get(cp, se, "name", "UNAVAILABLE"),
                'app'     : cp_get(cp, 'osg_dirs', 'app', '/Unknown'),
                "data"    : cp_get(cp, "osg_dirs", "data", "/Unknown"),
                }
            printTemplate(VOView, info)

def main():
    try:
        cp = config()
        condor_path = cp_get(cp, "condor", "condor_path", None)
        if condor_path != None:
            addToPath(condor_path)
        vo_map = VoMapper(cp)
        condorVersion = getLrmsInfo(cp) 
        total_nodes, claimed, unclaimed = print_CE(cp)
        print_VOViewLocal(cp)
    except Exception, e:
        log.error(e)
        raise

if __name__ == '__main__':
    main()

