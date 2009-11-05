#!/usr/bin/python

import os
import sys
import time
import pickle
import urllib2
import calendar
import xml.dom.minidom

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_cese_bind import getCEList, getSEList
from gip_common import config, cp_get, getLogger, getTemplate, \
    printTemplate

log = getLogger("GIP.Downtime")

default_url = "https://myosg.grid.iu.edu/rgdowntime/xml?datasource=downtime" \
    "&all_resources=on"

# If the GlueDowntime template is not available, we will fall back to these
# entries.  This is done so downtime.py can be a standalone plugin on existing
# GIP installs
ce_template = """\
dn: GlueCEUniqueID=%(ceUniqueID)s,mds-vo-name=local,o=grid
GlueCEStateStatus: %(status)s
"""

se_template = """\
dn: GlueSEUniqueID=%(seUniqueID)s,mds-vo-name=local,o=grid
GlueSEStatus: %(status)s
"""

class Downtime(object):

    def __init__(self):
        self.services_affected = []
        self.resource_group = None
        self.resource = None
        self.fqdn = None
        self.start = None
        self.end = None

    date_format = "%b %d, %Y %H:%M:%S %Z"
    def parse(self, dom):
        self.resource_group = str(dom.getElementsByTagName("GroupName")[0].\
            firstChild.data)
        self.resource = str(dom.getElementsByTagName("ResourceName")[0].\
            firstChild.data)
        self.fqdn = str(dom.getElementsByTagName("ResourceFQDN")[0].\
            firstChild.data)
        start_str = str(dom.getElementsByTagName("StartTime")[0].\
            firstChild.data)
        self.start = time.strptime(start_str, self.date_format)
        self.start = calendar.timegm(self.start)
        end_str = str(dom.getElementsByTagName("EndTime")[0].firstChild.data)
        self.end = time.strptime(end_str, self.date_format)
        self.end = calendar.timegm(self.end)
        for service_dom in dom.getElementsByTagName("Name"):
            service = str(service_dom.firstChild.data)
            if service not in self.services_affected:
                self.services_affected.append(service)

    def isActive(self, now=time.time()):
        if now >= self.start and now < self.end:
            return True

    def matchCE(self, host):
        if host == self.fqdn:
            return True

    def matchResource(self, resource):
        if resource == self.resource:
            return True

    def printDowntime(self):
        print "{resource_group:%s, resource:%s, fqdn:%s, start:%s, end:%s}" % \
            (self.resource_group, self.resource, self.fqdn, self.start, self.end)
        
class ExternalDowntimePlugin(object):

    def __init__(self, cp):
        self.cp = cp
        self.downtimes = []

    def downloadDowntimes(self):
        downtime_url = cp_get(self.cp, "gip", "downtime_url", default_url)
        try:
            fd = urllib2.urlopen(downtime_url)
        except Exception, e:
            log.warning("Unable to download downtime information from url %s.")
            log.exception(e)
            log.warning("Will attempt to fall-back to last recorded " \
                "information")
            return None
        return fd

    def loadGood(self):
        temp_dir = os.path.expandvars(cp_get(self.cp, "gip", "temp_dir", \
            "$GIP_LOCATION/var/tmp"))
        good = os.path.join(temp_dir, "known_downtime.pickle")
        if not os.path.exists(good):
            log.info("No previously recorded downtimes available.")
            return

        try:
            fd = open(good, 'r')
        except Exception, e:
            log.warning("Unable to load last known good downtimes.")
            log.exception(e)
            log.warning("Will not be able to fall back to this file (%s)." % \
                good)
            return

        try:
            downtimes = pickle.load(fd)
        except Exception, e:
            log.warning("Unable to parse the binary file of known downtimes.")
            log.exception(e)
            log.warning("Will not be able to fall back to this file (%s)." % \
                good)
            return
        return downtimes

    def saveGood(self, downtimes):
        temp_dir = os.path.expandvars(cp_get(self.cp, "gip", "temp_dir", \
            "$GIP_LOCATION/var/tmp"))
        good = os.path.join(temp_dir, "known_downtime.pickle")
        try:
            fd = open(good, 'w')
        except Exception, e:
            log.warning("Unable to open save file (%s) for downtimes." % good)
            log.exception(e)
            log.warning("Will not be able to save these for the future.")
            return

        try:
            pickle.dump(downtimes, fd)
        except Exception, e:
            log.warning("Error pickling the downtime object.")
            log.exception(e)
            log.warning("Non-fatal, but will not be able to fall-back to this" \
                " file in the future.")

    def parseDowntimes(self, fd):
        try:
            dom = xml.dom.minidom.parse(fd)
        except Exception, e:
            log.warning("Unable to parse given downtime information.")
            log.exception(e)
            raise
        downtimes = []
        for downtime_dom in dom.getElementsByTagName("Downtime"):
            downtime = Downtime()
            downtime.parse(downtime_dom)
            downtimes.append(downtime)
        log.info("There were %i parsed downtimes found (for all resources on" \
            " grid)" % len(downtimes))
        return downtimes

    def getCurrentDowntimes(self):
        downtime_file = cp_get(self.cp, "gip", "downtime_file", None)
        if downtime_file:
            if os.path.exists(downtime_file):
                log.info("Will try to get downtimes from file %s." % \
                    downtime_file)
                try:
                    fd = open(downtime_file, 'r')
                except OSError, oe:
                    log.warning("Unable to load downtime file %s." % \
                        downtime_file)
                    log.exception(oe)
                    fd = None
            else:
                fd = None
                log.warning("Given downtime file, %s, does not exist." % \
                    downtime_file)
        else:
            fd = self.downloadDowntimes()
        cur_down = None
        if fd:
            try:
                cur_down = self.parseDowntimes(fd)
            except Exception:
                log.warning("Unable to parse current downtimes; will attempt" \
                    " to fall back to save file")
        else:
            log.warning("Unable to get current downtimes; will attempt to " \
                "fall back to save file")
        return cur_down

    def run(self):
        cur_down = self.getCurrentDowntimes()
        prev_down = self.loadGood()
        if cur_down != None:
            self.downtimes = cur_down
            self.saveGood(cur_down)
        elif prev_down != None:
            self.downtimes = prev_down
        else:
            raise Exception("Unable to get any downtime information")

    def publishCEList(self):
        try:
            template = getTemplate("GlueDowntime", "GlueCEUniqueID")
        except:
            template = ce_template
        for downtime in self.downtimes:
            if not downtime.isActive():
                continue
            for ce in getCEList(self.cp):
                hostname = ce.split(':')[0].split("/")[0]
                if downtime.matchCE(hostname):
                    info = {'ceUniqueID': ce, 'status': "Closed"}
                    printTemplate(template, info)

    def publishSEList(self):
        try:
            template = getTemplate("GlueDowntime", "GlueSEUniqueID")
        except:
            template = se_template
        for downtime in self.downtimes:
            if not downtime.isActive():
                continue
            for se in getSEList(self.cp):
                test_name = se
                if se.endswith("_classicSE"):
                    test_name = se[:-10]
                hostname = test_name.split(':')[0].split("/")[0]
                if downtime.matchCE(hostname) or \
                        downtime.matchResource(hostname):
                    info = {'seUniqueID': se, 'status': 'Closed'}
                    printTemplate(template, info)

def main():
    cp = config()
    handler = ExternalDowntimePlugin(cp)
    try:
        handler.run()
    except Exception, e:
        log.exception(e)
        log.error("Unable to determine downtime information!  Not making any" \
            " changes to the site information.")
        sys.exit(2)
    try:
        handler.publishCEList()
    except Exception, e:
        log.exception(e)
    try:
        handler.publishSEList()
    except Exception, e:
        log.exception(e)

if __name__ == '__main__':
    main()

