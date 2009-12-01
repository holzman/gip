
import os
import pwd
import sys
import time

from gip_common import cp_get
from gip_logging import getLogger
from gip_sections import site, ce

log = getLogger("GIP.Gratia")

# Try to load up the Gratia Services modules.
# If successful, the GIP has the capability to send information to Gratia.
# The information we can send to Gratia is ultimately above and beyond the info
# which we can fit in the BDII schema.
has_gratia_capacity = True
try:
    # Try hard to bootstrap paths.
    paths = ['/opt/vdt/gratia/probe/common', '$VDT_LOCATION/gratia/probe/' \
        'common', '/opt/vdt/gratia/probe/services', '$VDT_LOCATION/gratia/' \
        '/probe/services']
    for path in paths:
        path = os.path.expandvars(path)
        if os.path.exists(path) and path not in sys.path:
            sys.path.append(path)

    # Try to import the necessary Gratia modules.
    import Gratia
    import ComputeElement
    import ComputeElementRecord
    import StorageElement
    import StorageElementRecord
    import Subcluster

    time_now = time.time()
except Exception, e:
    has_gratia_capacity = False
    log.warning("Could not import the Gratia Service modules.")
    log.warning("Non-fatal error: %s" % str(e))

def initialize(cp):
    if has_gratia_capacity:
        try:
            locs = ['$VDT_LOCATION/gratia/probe/services/ProbeConfig',
                '/opt/vdt/gratia/probe/services/ProbeConfig']
            probeConfig = None
            for loc in locs:
                try:
                    loc = os.path.expandvars(loc)
                    open(loc, 'r').read()
                    probeConfig = loc
                    break
                except:
                    pass
            if not probeConfig:
                try:
                    uid = os.geteuid()
                    name = pwd.getpwuid(uid).pw_name
                except:
                    raise Exception("Unable to locate a suitable " \
                        "ProbeConfig")
                raise Exception("User %s unable to locate / read a " \
                    " ProbeConfig file; checked locations %s" % \
                    (name, ', '.join(locs)))
            log.debug("Attempting to initialize Gratia using config %s" % \
                probeConfig)
            old_stdout = sys.stdout
            sys.stdout = sys.stderr
            try:
                Gratia.Initialize(probeConfig)
            finally:
                sys.stdout = old_stdout
            siteName = cp_get(cp, site, "name", "UNKNOWN")
            ce_name = cp_get(cp, ce, "name", "UNKNOWN_CE")
            hostName = cp_get(cp, ce, "host_name", ce_name)
            probeName = 'gip_CE:%s' % hostName
            Gratia.Config.setSiteName(site)
            Gratia.Config.setMeterName(probeName)
            log.info("Enabled GIP-Gratia reporting")
        except Exception, e:
            log.warning("Non-fatal exception while initializing GIP" \
                "-Gratia link: %s" % str(e))

def ce_record(cp, info):
    if has_gratia_capacity:
        try:
            desc = ComputeElement.ComputeElement()
            desc.UniqueID(info['ceUniqueID'])
            desc.CEName(info['queue'])
            desc.Cluster(info['hostingCluster'])
            desc.HostName(info['hostName'])
            desc.Timestamp(time_now)
            desc.LrmsType(info['lrmsType'])
            desc.LrmsVersion(info['lrmsVersion'])
            desc.MaxRunningJobs(info['max_running'])
            desc.MaxTotalJobs(info['max_total'])
            desc.AssignedJobSlots(info['assigned'])
            desc.Status(info['status'])
            old_stdout = sys.stdout
            sys.stdout = sys.stderr
            result = None
            try:
                result = Gratia.Send(desc)
            finally:
                sys.stdout = old_stdout
            result = Gratia.Send(desc)
            log.debug("CE description for Gratia: %s" % desc)
            log.debug("Gratia sending result: %s" % result)
        except Exception, e:
            log.warning("Non-fatal exception occurred during formation of" \
                " Gratia CE record: %s" % str(e))

def vo_record(cp, info):
    if has_gratia_capacity:
        try:
            log.debug("Starting creation of VOView record.")
            cer = ComputeElementRecord.ComputeElementRecord()
            cer.UniqueID(info['ceUniqueID'])
            cer.VO(info['voLocalID'])
            cer.Timestamp(time_now)
            try:
                if int(info['running']) == 0 and int(info['total']) \
                        == 0 and int(info['waiting']) == 0:
                    log.debug("Skipping VO record because VO %s is inactive" \
                        % info['voLocalID'])
                    return
            except Exception, e:
                log.warning("Non-fatal exception while skipping empty VO " \
                    "record: %s" % str(e))
                return
            cer.RunningJobs(info['running'])
            cer.TotalJobs(info['total'])
            cer.WaitingJobs(info['waiting'])
            old_stdout = sys.stdout
            sys.stdout = sys.stderr
            result = None
            try:
                result = Gratia.Send(cer)
            finally:
                sys.stdout = old_stdout
            log.debug("Gratia description of VOView: %s" % str(cer))
            log.debug("Gratia sending result: %s" % result)
        except Exception, e:
            log.warning("Non-fatal exception while sending VOView " \
                "to Gratia: %s" % str(e))

