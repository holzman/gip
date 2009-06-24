
"""
Functions which generate cluster and subcluster information for this GIP
install.

The GLUE cluster entry should represent the details of a compute cluster 
(possibly heterogeneous).

The GLUE subcluster represents a subset of the cluster which is homogeneous
hardware.
"""

import re
import os

from gip_common import cp_get, cp_getInt, ldap_boolean, cp_getBoolean, \
    notDefined, getLogger, voList
from gip_testing import runCommand
from gip_sections import cluster, subcluster, ce

__all__ = ['generateGlueCluster', 'generateSubClusters', 'getClusterName', \
    'getClusterID', 'getOSGVersion', 'getReferenceSI00']
__author__ = 'Brian Bockelman'

log = getLogger("GIP.Cluster")

_referenceSI00 = None
def getReferenceSI00(cp):
    """
    The WLCG Installed Capacity document requires us to publish, for every CE,
    the "reference" SI00 of a core for the CE.  As we do not track the CE to
    subcluster mapping, we just return the minimum SI00 of all the cluster
    CPUs.
    """
    global _referenceSI00
    if _referenceSI00:
        return _referenceSI00
    _referenceSI00 = 2000
    for section in cp.sections():
        if not section.startswith(subcluster):
            continue
        _referenceSI00 = min(cp_getInt(cp, section, "SI00", 2000),
            _referenceSI00)
    return _referenceSI00

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

def getOSGVersion(cp):
    """
    Returns the running version of the OSG
    """
    osg_ver_backup = cp_get(cp, "ce", "osg_version", "OSG 1.2.0")
    osg_version_script = cp_get(cp, "gip", "osg_version_script",
        "")
    if len(osg_version_script) == 0:
        osg_version_script = '$VDT_LOCATION/osg-version'
        osg_version_script = os.path.expandvars(osg_version_script)
        if not os.path.exists(osg_version_script):
            osg_version_script = os.path.expandvars("$VDT_LOCATION/osg/bin/" \
                "osg-version")
    osg_version_script = os.path.expandvars(osg_version_script)
    try:
        osg_ver = runCommand(osg_version_script).read().strip()
    except Exception, e:
        log.exception(e)
        osg_ver = ''
    if len(osg_ver) == 0:
        osg_ver = osg_ver_backup
    return osg_ver

def getClusterName(cp):
    """
    Return the name of the associated cluster.
    """
    ce_name = cp_get(cp, ce, 'name', "UNKNOWN_CE")
    simple = cp.getboolean(cluster, 'simple')
    if simple:
        return ce_name
    else:
        return cp.get(cluster, 'name')

def getClusterID(cp):
    """
    Return the unique ID of the associated cluster.
    """
    ce_name = cp_get(cp, ce, 'unique_name', \
        cp_get(cp, ce, 'name', "UNKNOWN_CE"))
    simple = cp.getboolean(cluster, 'simple')
    if simple:
        return ce_name
    else:
        return cp_get(cp, cluster, 'name', ce_name)

#def generateGlueCluster(cp):
#    """
#    Generate cluster information from the site's configuration.
#    """

def _generateSubClusterHelper(cp, section):
    """
    Private helper function for generateSubClusters; do not use.
    
    Implementation note: We create a bunch of variables but never
       appear to use them - we actually do return the local dictionary
       at the end of the function.  Just being syntactically lazy.
    """
    #pylint: disable-msg=W0612
    # Names
    subCluster = cp_get(cp, section, "name", cluster)
    subClusterUniqueID = cp_get(cp, section, "unique_name", subCluster)
    clusterUniqueID = getClusterID(cp)

    # Host statistics
    clockSpeed = cp_getInt(cp, section, "cpu_speed_mhz", 999999999)
    cpuCount = cp_getInt(cp, section, "cpus_per_node", 2)
    model = cp_get(cp, section, "cpu_model", 'UNDEFINEDVALUE')
    platform = cp_get(cp, section, "platform", "UNKNOWN")
    vendor = cp_get(cp, section, "cpu_vendor", 'UNDEFINEDVALUE')
    cores_per_cpu = cp_getInt(cp, section, "cores_per_cpu", 2)
    si2k = cp_getInt(cp, section, "SI00", 2000)
    sf2k = cp_getInt(cp, section, "SF00", 2000)
    ram = cp_getInt(cp, section, "ram_size", 1000*cpuCount*cores_per_cpu)
    cores = cp_getInt(cp, section, "total_cores", 999999999)
    if cores_per_cpu != 0:
        cpus = cp_getInt(cp, section, "total_cpus", cores/cores_per_cpu)
    else:
        cpus = 0
    virtualMem = ram + cp_getInt(cp, section, "swap_size", 0)
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

    app_attr = 'GlueHostApplicationSoftwareRunTimeEnvironment'
    apps = getApplications(cp)
    applications = '\n'.join(['%s: %s' % (app_attr, i['locationId']) for i in \
        apps if i['locationId']])
    applications += '\n'
        
    # BDII stuff
    bdii = cp_get(cp, "bdii", "endpoint", "ldap://is.grid.iu.edu:2170")

    return locals()

def generateSubClusters(cp):
    """
    Generate subcluster information from the site's configuration.

    The following attributes are used:
       - name
       - unique_name
       - cores_per_cpu
       - SI00
       - SF00
       - cpu_speed_mhz
       - cpu_model
       - cpu_vendor
       - swap_size
       - inbound_network
       - outbound_network
       - total_cores
       - total_cpus

    """
    subclusters = []
    for sect in cp.sections():
        if sect.startswith(subcluster):
            subclusters.append(_generateSubClusterHelper(cp, sect))
    return subclusters

def getSubClusterIDs(cp):
    """
    Return a list of the subcluster unique ID's for this configuration.
    """
    subclusters = []
    for section in cp.sections():
        if not section.startswith(subcluster):
            continue
        subCluster = cp_get(cp, section, "name", "UNKNOWN")
        subclusters.append(cp_get(cp, section, "unique_name", subCluster))
    return subclusters

def getApplications(cp):
    """
    Return a list of dictionaries containing the application info for each
    application installed at this site.
    
    Each returned dictionary should have the following keys:
        - locationId
        - locationName
        - version
        - path
        
    @param cp: Site configuration
    @return: List of dictionaries; each dictionary contains the information
    about a specific installed application.
    """
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
            # Skip blank lines and comments
            if len(line) == 0 or info[0].startswith('#'):
                continue
            if len(info) != 3:
                log.warning("Invalid line: %s" % line)
                continue
            if info[1].startswith('#') or info[1].startswith('$'):
                info[1] = 'UNDEFINED'
            info = {'locationName': info[0], 'version': info[1], 'path':info[2]}
            info['locationId'] = info['locationName']
            locations.append(info)
    osg_ver = getOSGVersion(cp)
    if osg_ver:
        info = {'locationId': osg_ver, 'locationName': osg_ver, 'version': \
            osg_ver, 'path': os.environ.get('VDT_LOCATION', '/UNKNOWN')}
        locations.append(info)
    try:
        locations += getApplicationsV1(cp)
    except Exception, e:
        log.exception(e)
    if not locations:
        locations = [{'locationId': "UNKNOWN", 'locationName': "UNKNOWN",
            'version': "UNKNOWN", 'path': '/UNKNOWN'}]
    return locations 

def getApplicationsV1(cp):
    """
    Retrieves the applications in the new "v1" format; it looks in

       - $OSG_APP/etc/grid3-locations.txt
       - $OSG_APP/etc/<vo>/locations-v1.txt

    Here, $OSG_APP/etc may be overridden by the config variable
    gip.software_dir.

    One directory is created per VO, owned by root, world-writable, and set to 
    sticky.  The VO names will be determined by the current methods (using 
    voList).  GIP will then scan the directory $OSG_APP/etc/<vo> for all the 
    VO names known to GIP, and read only a file named locations-v1.txt.  It is 
    assumed that locations-v1.txt is the same format as the grid3-locations.txt.
    """
    app_dir = cp_get(cp, "osg_dirs", "app", "/UNKNOWN")
    base_path = os.path.join(app_dir, "etc")
    base_path = cp_get(cp, "gip", "software_dir", base_path)
    locations = []
    for vo in voList(cp):
        vo_dir = os.path.join(base_path, vo)
        vo_dir = os.path.expandvars(vo_dir)
        vo_path = os.path.join(vo_dir, 'locations-v1.txt')
        if not os.path.exists(vo_path):
            continue
        try:
            fp = open(vo_path, 'r')
        except:
            log.warning("Unable to read VO application file: %s" % vo_path)
            continue
        for line in fp:
            line = line.strip()
            info = line.split()
            # Skip blank lines and comments
            if len(line) == 0 or info[0].startswith('#'):
                continue
            if len(info) != 3:
                log.warning("Invalid line: %s" % line)
                continue
            if info[1].startswith('#') or info[1].startswith('$'):
                info[1] = 'UNDEFINED'
            info = {'locationName': info[0], 'version': info[1], 'path':info[2]}
            info['locationId'] = info['locationName']
            locations.append(info)
    return locations
            
