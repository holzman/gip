
"""
Provide information about the CEs of this site, the SEs, and
the bindings between the two.
"""

from gip_common import cp_get, cp_getBoolean
from gip_storage import getPath
from pbs_common import getQueueList as getPBSQueueList
from lsf_common import getQueueList as getLSFQueueList
from condor_common import getQueueList as getCondorQueueList
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
        queue_entries = getPBSQueueList(cp)
    elif jobman == 'lsf':
        queue_entries = getLSFQueueList(cp)
    elif jobman == 'condor':
        queue_entries = getCondorQueueList(cp)
    else:
        raise ValueError("Unknown job manager %s." % jobman)
    for queue in queue_entries:
        ce_list.append(ce_name % queue)
    return ce_list

def getClassicSEList(cp):
    """
    Return a list of all the ClassicSE's at this site

    @param cp: Site configuration
    @returns: List of all the ClassicSE's unique_ids
    """
    if not cp_getBoolean(cp, "classic_se", "advertise_se", False):
        return []
    classicSE = cp_get(cp, "classic_se", "host", None)
    if not classicSE: # len(classicSE) == 0 or classicSE == None
        return []
    return [classicSE]

def getSEList(cp, classicSEs=True):
    """
    Return a list of all the SE's at this site.

    @param cp: Site configuration.
    @keyword classicSEs: Return list should contain classicSEs; default is True.
    @returns: List of strings containing all the local SE unique_ids.
    """
    simple = cp.getboolean(cesebind, 'simple')
    se_list = []
    if simple:
        try:
            se_list = [cp.get(se, 'unique_name')]
        except:
            pass
        for sect in cp.sections():
            if not sect.lower().startswith('se'):
                continue
            try:
                se_list += [cp.get(sect, 'unique_name')]
            except:
                pass
    else:
        se_list = eval(cp.get(cesebind, 'se_list'), {})
    if classicSEs:
        se_list.extend(getClassicSEList(cp))
    return se_list

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
    se_list = getSEList(cp, classicSEs = False)
    classicse_list = getClassicSEList(cp)
    se_list.extend(classicse_list)
    #access_point = cp_get(cp, "vo", "default", "/")
    access_point = getPath(cp)
    if not access_point:
        access_point = "/"
    classic_access_point = cp_get(cp, "osg_dirs", "data", "/")
    for myce in ce_list:
        for myse in se_list:
            if myse in classicse_list:
                ap = classic_access_point
            else:
                ap = access_point
            info = {'ceUniqueID' : myce,
                    'seUniqueID' : myse,
                    'access_point' : ap,
                   }
            binds.append(info)
    return binds

