#!/usr/bin/env python

import os
import sys
import signal
import socket
import urlparse
import time
from xml.dom.minidom import parseString

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger, runCommand, voList

log = getLogger("GIP.gt4")

wsrf_cmd = """wsrf-query -a -s %s --key '{http://www.globus.org/namespaces/2004/10/gram/job}ResourceID' Fork "//*[local-name()='ServiceMetaDataInfo']\""""

ns1 = "http://mds.globus.org/metadata/2005/02"

def fixUrl(url):
    parts = list(urlparse.urlsplit(url))
    if parts[1].find(":") >= 0:
        host, port = parts[1].split(":")
        host = socket.getfqdn(host)
        parts[1] = "%s:%s" % (host, port)
    else:
        parts[1] = socket.getfqdn(parts[1])
    return urlparse.urlunsplit(parts)

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
        dom = parseString(info)
        startTime = str(dom.getElementsByTagNameNS(ns1, "startTime")[0].\
                        firstChild.data)
        version = str(dom.getElementsByTagNameNS(ns1, "version")[0].firstChild.\
                      data)
    except:
        startTime = "1970-01-01T00:00:00Z"
        version = "UNKNOWN"

    acbr = ''
    for vo in voList(cp):
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
            "startTime"   : startTime,
            "semantics"   : "UNKNOWN",
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


