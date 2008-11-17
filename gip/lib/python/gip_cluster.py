
"""
Functions which generate cluster and subcluster information for this GIP
install.

The GLUE cluster entry should represent the details of a compute cluster 
(possibly heterogeneous).

The GLUE subcluster represents a subset of the cluster which is homogeneous
hardware.
"""
import os
from gip_sections import cluster, subcluster, ce
from gip_common import cp_get, getLogger
from gip_testing import runCommand

__all__ = ['generateGlueCluster', 'generateSubClusters', 'getClusterName', \
    'getClusterID']
__author__ = 'Brian Bockelman'

log = getLogger("GIP.Cluster")

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
    pass

def _generateSubClusterHelper(cp, sect):
    """
    Private helper function for generateSubClusters; do not use.
    """

def generateSubClusters(cp):
    """
    Generate subcluster information from the site's configuration.
    """
    subclusters = []
    for sect in cp.sections:
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
            if len(info) != 3 or info[0].startswith('#'):
                continue
            if info[1].startswith('#') or info[1].startswith('$'):
                info[1] = 'UNDEFINED'
            info = {'locationName': info[0], 'version': info[1], 'path':info[2]}
            info['locationId'] = info['locationName']
            locations.append(info)
    osg_ver = getOSGVersion(cp)
    if osg_ver:
        info = {'locationId': osg_ver, 'locationName': osg_ver, 'version': \
            'osg_ver', 'path': os.environ.get('VDT_LOCATION', '/UNKNOWN')}
        locations.append(info)
    return locations

def getOSGVersion(cp):
    """
    Returns the running version of the OSG
    """
    osg_ver_backup = cp_get(cp, "ce", "osg_version", "OSG 1.0.0")
    try:
        if os.environ['VDT_LOCATION'] not in os.environ['PATH'].split(':'):
            os.environ['PATH'] += ':' + os.environ['VDT_LOCATION']
        osg_ver = runCommand('osg-version').read().strip()
    except Exception, e:
        log.exception(e)
        osg_ver = ''
    if len(osg_ver) == 0:
        osg_ver = osg_ver_backup
    return osg_ver