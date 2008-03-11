
"""
Functions which generate cluster and subcluster information for this GIP
install.

The GLUE cluster entry should represent the details of a compute cluster 
(possibly heterogeneous).

The GLUE subcluster represents a subset of the cluster which is homogeneous
hardware.
"""

from gip_sections import cluster, subcluster, ce

__all__ = ['generateGlueCluster', 'generateSubClusters', 'getClusterName', \
    'getClusterID']
__author__ = 'Brian Bockelman'

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

