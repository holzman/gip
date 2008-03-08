
from gip_common import voList
from pbs_common import getQueueInfo
from gip_sections import ce, cesebind

def getCEList(cp):
    """
    Return a list of all the CE names at this site.

    If WS-GRAM is installed, this might additionally return some WS-GRAM
    entries (this feature is not yet implemented).

    @param cp: Site configuration
    @returns: List of strings containing all the local CE names.
    """
    jobman = cp.get(ce, "job_manager").strip()
    hostname = cp.get(ce, 'name')
    ce_name = '%s:2119/jobmanager-%s-%%s' % (hostname, jobman)
    ce_list = []
    if jobman == 'pbs':
        queue_entries = getQueueInfo(cp)
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
        return cp.get(se, 'unique_name')
    else:
        return eval(cp.get(cesebind, 'se_list'), {})

