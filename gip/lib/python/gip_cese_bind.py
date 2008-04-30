
import sys

from gip_common import voList, cp_get
from pbs_common import getQueueList
from gip_sections import ce, cesebind, se

def getCEList(cp):
    """
    Return a list of all the CE names at this site.

    If WS-GRAM is installed, this might additionally return some WS-GRAM
    entries (this feature is not yet implemented).

    @param cp: Site configuration
    @returns: List of strings containing all the local CE names.
    """
    jobman = cp.get(ce, "job_manager").strip().lower()
    hostname = cp.get(ce, 'name')
    ce_name = '%s:2119/jobmanager-%s-%%s' % (hostname, jobman)
    ce_list = []
    if jobman == 'pbs':
        queue_entries = getQueueList(cp)
        for queue in queue_entries:
            ce_list.append(ce_name % queue)
    else:
        for vo in getVoList(cp):
             ce_list.append(ce_name % vo)
    return ce_list

def getSEList(cp):
    """
    Return a list of all the SE's at this site.

    @param cp: Site configuration.
    @returns: List of strings containing all the local SE names.
    """
    simple = cp.getboolean(cesebind, 'simple')
    if simple:
        return [cp.get(se, 'unique_name')]
    else:
        return eval(cp.get(cesebind, 'se_list'), {})

def getCESEBindInfo(cp):
    """
    Generates a list of information for the CESE bind groups.

    Each list entry is a dictionary containing the necessary information for
    filling out a CESE bind entry.

    @param cp: Site configuration
    @returns: List of dictionaries; each dictionary is a CESE bind entry.
    """
    binds = []
    ce_list = getCEList(cp)
    se_list = getSEList(cp)
    access_point = cp_get(cp, "vo", "default", "/")
    for ce in ce_list:
        for se in se_list:
            info = {'ceUniqueID' : ce,
                    'seUniqueID' : se,
                    'access_point' : access_point,
                   }
            binds.append(info)
    return binds

