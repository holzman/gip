
import os
import re
import sys

if 'GIP_LOCATION' in os.environ:
    sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
import gip_cluster

from gip_common import config, cp_get, cp_getBoolean, getLogger, getTemplate, \
    printTemplate
from gip_testing import runCommand
from gip_sections import *
from gip_cese_bind import getCEList
from gip_cluster import getClusterName, getClusterID

log = getLogger("GIP.Cluster")

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
        if 'hepspec' in subcluster_info and subcluster_info['hepspec']:
            desc = 'GlueHostProcessorOtherDescription: ' \
                'Cores=%s, Benchmark=%s-HEP-SPEC06' % \
                (str(subcluster_info['cores']), str(subcluster_info['hepspec']))
        else:
            desc = 'GlueHostProcessorOtherDescription: Cores=%s' % \
                str(subcluster_info['cores'])
        subcluster_info['otherDesc'] = desc
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
