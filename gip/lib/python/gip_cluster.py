
"""
Functions which generate cluster and subcluster information for this GIP
install.

The GLUE cluster entry should represent the details of a compute cluster 
(possibly heterogeneous).

The GLUE subcluster represents a subset of the cluster which is homogeneous
hardware.
"""

from gip_sections import cluster, subcluster

__all__ = [generateGlueCluster, generateSubClusters]
__author__ = 'Brian Bockelman'

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
            subclusters.append(_generateSubClusterHelper(cp, sect)
    return subclusters

