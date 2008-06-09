
import re

from gip_common import config, cp_get, cp_getBoolean, getLogger, runCommand
from gip_sections import *
from gip_cese_bind import getCEList

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
        raise Exception("Could not determine cluster name.")
    clusterUniqueID = cp_get(cp, 'ce', 'unique_name', cluster_name)
    siteUniqueID = cp.get(cp, "site", "unique_name")
    ces = getCEList(cp)
    glueClusters = ''
    for ce in ces:
        glueClusters += 'GlueForeignKey: GlueCEUniqueID=%s" %  ce
    bdii = cp.get('gip', 'bdii')
    info = { \
        'cluster': cluster_name,
        'clusterUniqueID': clusterUniqueID,
        'tmp': cp_get(cp, "osg_dirs", "tmp", cp_get(cp, "osg_dirs", "data", \
             "/tmp")),
        'wn_tmp': cp_get(cp, "osg_dirs", "wn_tmp", "/tmp"),
        'siteUniqueID": siteUniqueID,
        'glueClusters': glueClusters,
        'bdii': bdii,
    }
    template = getTemplate("GlueCluster", "GlueClusterUniqueID")
    printTemplate(template, info)
    
def print_subcluster(cp, cluster, section):
    # Names
    name = cp.get(section, "name")
    uniqueID = cp.get(section

    # Host statistics
    cpu_count = cp_getInt(section, "cpus_per_node", 2)
    cores_per_cpu = cp_getInt(section, "cores_per_cpu", 2)
    si2k = cp_getInt(cp, section, "SI00", 2000)
    sf2k = cp_getInt(cp, section, "SF00", 2000)
    ram = cp_getInt(cp, section, "ram_size", 1000*cpu_count*cores_per_cpu)
    virtualMem = cp_getInt(cp, section, "swap_size", 0)
    inboundIP = cp_getBoolean(cp, section, "inbound_network", False)
    outboundIP = cp_getBoolean(cp, section, "outbound_network", True)
    

    # Temp directories
    default_tmp = cp_get(cp, "osg_dirs", "tmp", cp_get(cp, "osg_dirs", "data", \
             "/tmp"))
    default_wn_tmp = cp_get(cp, "osg_dirs", "wn_tmp", "/tmp")
    tmp = cp_get(cp, section, "tmp", default_tmp)
    if notDefined(tmp):
        tmp = default_tmp



def print_subclusters(cp):
    subclusters = 
    for 

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
