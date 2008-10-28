
import os
import sys
import glob
import sets

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))

from gip_common import cp_get, getLogger, config
from gip_ldap import read_ldap, prettyDN

log = getLogger("GIP.ERT")

def load_static_ldif(cp, static_dir):
    ldap = sets.Set()
    for filename in glob.glob("%s/*.ldif" % static_dir):
        log.debug("Reading static file %s" % filename)
        try:
            my_ldap = read_ldap(open(filename, 'r').read())
        except:
            log.error("Unable to read %s." % filename)
        ldap.union(my_ldap)
    return ldap

def responseTimes(cp, running, waiting, average_job_time=None,
        max_job_time=None):
    """
    Computes the estimated and worst-case response times based on a simple
    formula.

    We take the
      ERT = average_job_time/(running+1)*waiting 
      WRT = max_job_time/(running+1)*waiting 

    If |running| + |waiting| < 10, then ERT=1hr, WRT=24hr unless |running|=0.
    If |running|=0 or |waiting|=0, then ERT=1 min.

    ERT and WRT must be positive; ERT maxes out at 1 day, WRT maxes out
    at 30 days.  WRT must be a minimum of 2*ERT.

    @param cp: Site configuration
    @param running: Number of jobs running
    @param waiting: Number of waiting jobs
    @keyword average_job_time: Average runtime (in seconds) for a job
    @keyword max_job_time: Maximum runtime (in seconds for a job
    @return: ERT, WRT (both are measured in seconds)
    """
    try:
        running = int(running)
    except:
        running = 0
    try:
        waiting = int(waiting)
    except:
        waiting = 0
    try:
        average_job_time = int(average_job_time)
    except:
        average_job_time = None
    try:
        max_job_time = int(max_job_time)
    except:
        max_job_time = None
    if average_job_time == None:
        average_job_time = cp_getInt(cp, 'gip', 'average_job_time', 4*3600)
    if max_job_time == None:
        max_job_time = cp_getInt(cp, 'gip', 'max_job_time', 24*3600)
    if max_job_time < average_job_time:
        max_job_time = 2*average_job_time
    if abs(running) + abs(waiting) < 10:
        if abs(running) == 0 or abs(waiting) == 0:
            return 60, 86400
        return 3600, 86400
    ERT = int(average_job_time/float(running+10)*waiting)
    WRT = int(max_job_time/float(running+1)*waiting)
    ERT = max(min(ERT, 86400), 0)
    WRT = max(min(WRT, 30*86400), 2*ERT)
    return ERT, WRT

def main():
    cp = config()
    static_directory = os.path.expandvars(cp_get(cp, "gip", "static_dir", \
        "$GIP_LOCATION/var/ldif"))
    static_ldif = load_static_ldif(cp, static_directory)
    for entry in static_ldif:
        if 'GlueCEState' not in entry.objectClass:
            continue
        ert, wrt = responseTimes(cp, entry['CEStateRunningJobs'],
            entry['CEStateWaitingJobs'])
        print prettyDN(entry)
        print "GlueCEStateEstimatedResponseTime: " + str(ert)
        print "GlueCEStateWorstResponseTime: " + str(wrt)
        print "\n"

if __name__ == '__main__':
    main()

