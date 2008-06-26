
"""
Functions which generate cluster and subcluster information for this GIP
install.

The GLUE cluster entry should represent the details of a compute cluster 
(possibly heterogeneous).

The GLUE subcluster represents a subset of the cluster which is homogeneous
hardware.
"""

import re

from gip_common import cp_get, cp_getInt, ldap_boolean, cp_getBoolean, \
    notDefined
from gip_testing import runCommand
from gip_sections import cluster, subcluster, ce

__all__ = ['generateGlueCluster', 'generateSubClusters', 'getClusterName', \
    'getClusterID']
__author__ = 'Brian Bockelman'

def getOsStatistics():
    """
    Gather statistics about the node's operating system

    NOTE: does not conform to the LSB layout that the GLUE schema suggests

    @returns: OS name, OS release, OS version
    """
    name = runCommand('uname').read().strip()
    release = runCommand('uname -r').read().strip()
    version = runCommand('uname -v').read().strip()
    return name, release, version

lsb_re = re.compile('Description:\s+(.*)\s+[Rr]elease\s+(.*)\s+\((.*)\)')
def getRelease():
    """
    Get the release information for the node; if the lsb_release command isn't
    found, return generic stats based on uname from getOsStatistics
    
    This function conforms to the suggestions made by the GLUE schema 1.3.

    @returns: OS name, OS release, OS version
    """
    m = lsb_re.match(runCommand('lsb_release -d').read())
    if m:
        return m.groups()
    else:
        return getOsStatistics()

def getClusterName(cp):
    """
    Return the name of the associated cluster.
    """
    ce_name = cp.get(ce, 'name')
    simple = cp.getboolean(cluster, 'simple')
    if simple:
        return ce_name
    else:
        return cp.get(cluster, 'name')

def getClusterID(cp):
    """
    Return the unique ID of the associated cluster.
    """
    ce_name = cp.get(ce, 'unique_name')
    simple = cp.getboolean(cluster, 'simple')
    if simple:
        return ce_name
    else:
        return cp.get(cluster, 'name')

def generateGlueCluster(cp):
    """
    Generate cluster information from the site's configuration.
    """

def _generateSubClusterHelper(cp, section):
    """
    Private helper function for generateSubClusters; do not use.
    """
    # Names
    subCluster = cp_get(cp, section, "name", cluster)
    subClusterUniqueID = cp_get(cp, section, "unique_id", subCluster)
    clusterUniqueID = cp_get(cp, cluster, "unique_id", cp_get(cp, cluster,
        'name', 'UNKNOWN'))

    # Host statistics
    clockSpeed = cp_getInt(cp, section, "clockspeed", 999999999)
    cpuCount = cp_getInt(cp, section, "cpus_per_node", 2)
    model = cp_get(cp, section, "model", 'UNDEFINEDVALUE')
    vendor = cp_get(cp, section, "vendor", 'UNDEFINEDVALUE')
    cores_per_cpu = cp_getInt(cp, section, "cores_per_cpu", 2)
    si2k = cp_getInt(cp, section, "SI00", 2000)
    sf2k = cp_getInt(cp, section, "SF00", 2000)
    ram = cp_getInt(cp, section, "ram_size", 1000*cpuCount*cores_per_cpu)
    cores = cp_getInt(cp, section, "cores", 999999999)
    cpus = cp_getInt(cp, section, "cores", cores/cores_per_cpu)
    virtualMem = cp_getInt(cp, section, "swap_size", 0)
    inboundIP = cp_getBoolean(cp, section, "inbound_network", False)
    outboundIP = cp_getBoolean(cp, section, "outbound_network", True)
    inboundIP = ldap_boolean(inboundIP)
    outboundIP = ldap_boolean(outboundIP)

    # OS Stats
    osName, osRelease, osVersion = getRelease()

    # Temp directories
    default_tmp = cp_get(cp, "osg_dirs", "tmp", cp_get(cp, "osg_dirs", "data", \
             "/tmp"))
    wn_tmp = cp_get(cp, "osg_dirs", "wn_tmp", "/tmp")
    tmp = cp_get(cp, section, "tmp", default_tmp)
    if notDefined(tmp):
        tmp = default_tmp

    #OSG Version
    osg_ver = cp_get(cp, "ce", "osg_version", "OSG 1.2.0")
    try:
        fp = open(os.path.expandvars('$VDT_LOCATION/osg-version'), 'r')
        osg_ver = fp.read().strip()
    except:
        pass
    applications = 'GlueHostApplicationSoftwareRunTimeEnvironment: %s\n' % \
        osg_ver
        
    # BDII stuff
    bdii = cp_get(cp, "bdii", "endpoint", "ldap://is.grid.iu.edu:2170")

    return locals()

def generateSubClusters(cp):
    """
    Generate subcluster information from the site's configuration.
    """
    subclusters = []
    for sect in cp.sections():
        if sect.startswith(subcluster):
            subclusters.append(_generateSubClusterHelper(cp, sect))
    return subclusters

def getApplications(cp):
    app_dir = cp_get(cp, "osg_dirs", "app", "/UNKNOWN")
    path1 = '%s/etc/grid3-locations.txt' % app_dir
    path2 = '%s/etc/osg-locations.txt' % app_dir
    paths = [path1, path2]
    path3 = cp_get(cp, "ce", "app_list", '')
    if path3:
        paths.append(path3)
    locations = []
    for path in paths:
        try:
            fp = open(path, 'r')
        except:
            continue
        for line in fp:
            line = line.strip()
            info = line.split()
            if len(info) != 3 or info[0].startswith('#'):
                continue
            if info[1].startswith('#') or info[1].startswith('$'):
                info[1] = 'UNDEFINED'
            info = {'locationName': info[0], 'version': info[1], 'path':info[2]}
            locations.append(info)
    return locations

