#!/usr/bin/env python

import os
import sys
import signal
import time
from xml.dom.minidom import parse

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger, runCommand

try:
    from gip_common import voList
except:
    def voList():
        return ['cms', 'osg', 'fmri', 'grase', 'gridex', 'ligo', 'ivdgl', 'gadu', 'GLOW', 'cdf', 'nanohub', 'dzero', 'sdss', 'gpn', 'engage', 'atlas']

log = getLogger("GIP.gt4")

wsrf_cmd = """wsrf-query -a -s %s --key '{http://www.globus.org/namespaces/2004/10/gram/job}ResourceID' Fork "//*[local-name()='version']\""""


def handler():
    raise Exception("Timeout occurred while waiting for the container's" \
                    " response")

def print_gt4(cp):
    use_gt4 = cp.getboolean("ce", "use_gt4")
    if not use_gt4:
        return

    serviceTemplate = getTemplate("GlueService", "GlueServiceUniqueID")
    endpoint = cp.get("ce", "gt4_endpoint")
    timeout = cp.getint("ce", "gt4_timeout")
    ce_name = cp.get("ce", "name")
    siteID = cp.get("site", "unique_name")
    uri = "%s/wsrf/services/ManagedJobFactoryService" % endpoint

    cmd = wsrf_cmd % uri
    stopwatch = 0.0
    info = ''
    try:
        fp = runCommand(cmd)

        stopwatch = -time.time()        
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(timeout)
        info = fp.read()
        signal.alarm(0)
        stopwatch += time.time()

        if fp.close() != None:
            raise RuntimeError("WSRF query failed against %s" % endpoint)
        working = True
        status = "OK"
        statusInfo = "Container responded after %.2f seconds." % stopwatch
    except Exception, e:
        log.error("Error during wsrf-query: %s" % str(e))
        if info != '':
            log.error("wsrf-query output:\n%s" % info)
        stopwatch += time.time()
        if abs(stopwatch - timeout) > 1:
            statusInfo = "Client-side failure: %s" % str(e)
            status = "Unknown"
        else:
            statusInfo = "Container query timed out after %i seconds." % timeout
            status = "Critical"

    try:
        dom = parseString(fp)
        version = str(dom.firstChild.firstChild.data)
    except:
        version = "UNKNOWN"

    acbr = ''
    for vo in voList():
        acbr += "GlueCEAccessControlBaseRule: VO:%s\n" % vo
    acbr = acbr[:-1]

    info = {"serviceName" : "%s WS-GRAM" % ce_name,
            "serviceID"   : uri,
            "endpoint"    : uri,
            "uri"         : uri,
            "url"         : uri,
            "serviceType" : "GRAM",
            "version"     : version,
            "wsdl"        : uri + "?wsdl",
            "status"      : status,
            "statusInfo"  : statusInfo,
            "acbr"        : acbr,
            "siteID"      : siteID,
           }

    print serviceTemplate % info

def main():
    try:
        cp = config()
        print_gt4(cp)
    except:
        sys.stdout = sys.stderr
        raise

if __name__ == '__main__':
    main()


