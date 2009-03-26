#!/usr/bin/python

import os
import re
import sys
import glob
import cStringIO

if 'GIP_LOCATION' not in os.environ and 'GLOBUS_LOCATION' in os.environ:
    os.environ['GIP_LOCATION'] = os.path.expandvars("$GLOBUS_LOCATION/../gip")
    os.environ['VDT_LOCATION'] = os.path.expandvars("$GLOBUS_LOCATION/..")

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))

from gip_common import cp_get, getLogger, config
from gip_ldap import read_ldap, prettyDN

log = getLogger("GIP.ERT")

bad_file_re = re.compile('.*\.ldif\.[0-9]+\.')

def fix_crappy_wrapper_output(fp):
    stanzas = []
    cur_ldif = ''
    for line in fp:
        if line.startswith('dn:'):
            if cur_ldif:
                stanzas.append(cur_ldif)
                cur_ldif = ''
        elif len(line.strip()) == 0:
            continue
        cur_ldif += line
    output_stanzas = []
    for i in stanzas:
        output_stanzas.append(cStringIO.StringIO(i+"\n"))
    return output_stanzas

def load_static_ldif(cp, static_dir):
    ldap = []
    old_stdout = sys.stdout
    dev_null = open('/dev/null', 'w')
    for filename in glob.glob("%s/*.ldif.*" % static_dir):
        if bad_file_re.match(filename):
            continue
        log.debug("Reading static file %s" % filename)
        try:
            my_ldap = []
            try:
                sys.stdout = dev_null
                for i in fix_crappy_wrapper_output(open(filename, 'r')):
                    try:
                        my_ldap += read_ldap(i)
                    except Exception, e:
                        print >> sys.stderr, e
                        continue
            finally:
                sys.stdout = old_stdout
        except SystemExit, KeyboardInterrupt:
            raise
        except Exception, e:
            log.error("Unable to read %s." % filename)
            log.exception(e)
            print >> sys.stderr, e
            continue
        ldap += my_ldap
    return ldap

def cp_getInt(cp, section, option, default):
    """
    Helper function for ConfigParser objects which allows setting the default.
    Returns an integer, or the default if it can't make one.

    @param cp: ConfigParser object
    @param section: Section of the config parser to read
    @param option: Option in section to retrieve
    @param default: Default value if the section/option is not present.
    @returns: Value stored in the CP for section/option, or default if it is
        not present.
    """
    try:
        return int(str(cp_get(cp, section, option, default)).strip())
    except:
        return default

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
    static_directory = os.path.expandvars(cp_get(cp, "gip", "temp_dir", \
        "$GIP_LOCATION/var/tmp"))
    static_ldif = load_static_ldif(cp, static_directory)
    for entry in static_ldif:
        if 'CEStateRunningJobs' not in entry.glue or \
                'CEStateWaitingJobs' not in entry.glue:
            continue
        running = entry.glue['CEStateRunningJobs']
        waiting = entry.glue['CEStateWaitingJobs']
        ert, wrt = responseTimes(cp, running, waiting)
        dn = "dn: " + ",".join(entry.dn)
        msg = "For DN %s, running %s, waiting %s, ert %i, wrt %i." % \
            (dn, running, waiting, ert, wrt)
        log.info(msg)
        print >> sys.stderr, msg
        print dn
        print "GlueCEStateEstimatedResponseTime: " + str(ert)
        print "GlueCEStateWorstResponseTime: " + str(wrt)
        print ""

if __name__ == '__main__':
    main()

