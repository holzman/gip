
import os
import re
import sys

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
import gip_cluster

from gip_common import config, cp_get, cp_getBoolean, getLogger, getTemplate, \
    printTemplate
from gip_testing import runCommand
from gip_sections import *
from gip_cese_bind import getCEList
from gip_cluster import getClusterName, getClusterID

log = getLogger("GIP.Cluster")

def getOsStatistics():
    """
    Gather statistics about the node's operating system

    NOTE: does not conform to the LSB layout that the GLUE schema suggests

    @returns: OS name, OS release, OS version
    """
    name = runCommand('uname').read()
    release = runCommand('uname -r').read()
    version = runCommand('uname -v').read()
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

def print_clusters(cp):
    cluster_name = cp_get(cp, 'cluster', 'name', None)
    if not cluster_name:
        cluster_name = cp_get(cp, 'ce', 'hosting_cluster', None)
    if not cluster_name:
        cluster_name = cp_get(cp, 'ce', 'unique_name', None)
    if not cluster_name:
        getClusterName(cp)
        #raise Exception("Could not determine cluster name.")
    #clusterUniqueID = cp_get(cp, 'ce', 'unique_name', cluster_name)
    clusterUniqueID = getClusterID(cp)
    siteUniqueID = cp_get(cp, "site", "unique_name", 'UNKNOWN_SITE')
    extraCEs = cp_get(cp, 'cluster', 'other_ces', [])
    if extraCEs:
        extraCEs = [x.strip() for x in extraCEs.split(',')]

    ces = getCEList(cp, extraCEs)

    glueClusters = ''
    for ce in ces:
        glueClusters += 'GlueForeignKey: GlueCEUniqueID=%s\n' %  ce
    bdii = cp_get(cp, 'gip', 'bdii', 'ldap://is.grid.iu.edu:2170')
    info = { \
        'cluster': cluster_name,
        'clusterUniqueID': clusterUniqueID,
        'tmp': cp_get(cp, "osg_dirs", "tmp", cp_get(cp, "osg_dirs", "data", \
             "/tmp")),
        'wn_tmp': cp_get(cp, "osg_dirs", "wn_tmp", "/tmp"),
        'siteUniqueID': siteUniqueID,
        'glueClusters': glueClusters,
        'bdii': bdii,
    }
    template = getTemplate("GlueCluster", "GlueClusterUniqueID")
    printTemplate(template, info)
    
def print_subclusters(cp):
    subclusters = gip_cluster.generateSubClusters(cp)
    template = getTemplate("GlueCluster", "GlueSubClusterUniqueID")
    for subcluster_info in subclusters:
        printTemplate(template, subcluster_info)

def main():
    cp = config()
    if not cp_getBoolean(cp, 'cluster', 'advertise_cluster', True):
        return
    try:
        print_clusters(cp)
    except Exception, e:
        log.exception(e)
    try:
        print_subclusters(cp)
    except Exception, e:
        log.exception(e)

if __name__ == '__main__':
    main()
