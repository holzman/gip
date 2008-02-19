#!/usr/bin/env python

import os
import sys
import signal
import socket
import urlparse
from xml.dom.minidom import parseString

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger, runCommand

try:
    from gip_common import voList
except:
    def voList():
        return ['cms', 'osg', 'fmri', 'grase', 'gridex', 'ligo', 'ivdgl', 'gadu', 'GLOW', 'cdf', 'nanohub', 'dzero', 'sdss', 'gpn', 'engage', 'atlas']

log = getLogger("GIP.gt4")

wsrf_cmd = """wsrf-query -a -s %s "/" """

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
    siteID = cp.get("site", "unique_name")
    uri = "%s/wsrf/services/ContainerRegistryService" % endpoint

    cmd = wsrf_cmd % uri
    info = ''
    try:
        fp = runCommand(cmd)

        signal.signal(signal.SIGALRM, handler)
        signal.alarm(timeout)
        info = fp.read()
        signal.alarm(0)

        if fp.close() != None:
            raise RuntimeError("WSRF query failed against %s" % endpoint)
    except Exception, e:
        log.error("Error during wsrf-query: %s" % str(e))
        raise

    dom = parseString(info)
    ns1 = "http://docs.oasis-open.org/wsrf/2004/06/" \
          "wsrf-WS-ServiceGroup-1.2-draft-01.xsd"
    ns2 = "http://axis.org"
    ns3 = "http://schemas.xmlsoap.org/ws/2004/03/addressing"
    for entry in dom.getElementsByTagNameNS(ns1, "Entry"):
        serviceName = str(entry.getElementsByTagNameNS(ns2, 'ServiceName')[0].\
                          firstChild.data)
        uri = str(entry.getElementsByTagNameNS(ns1, "MemberServiceEPR")[0].\
              getElementsByTagNameNS(ns3, "Address")[0].firstChild.data)
        uri = fixUrl(uri)

        acbr = ''
        for vo in voList():
            acbr += "GlueCEAccessControlBaseRule: VO:%s\n" % vo
        acbr = acbr[:-1]

        info = {"serviceName" : serviceName,
                "serviceID"   : uri,
                "endpoint"    : uri,
                "uri"         : uri,
                "url"         : uri,
                "serviceType" : "GT4",
                "version"     : "UNKNOWN",
                "wsdl"        : uri + "?wsdl",
                "status"      : "UNKNOWN",
                "statusInfo"  : "UNKNWON",
                "acbr"        : acbr,
                "siteID"      : siteID,
                "startTime"   : "1970-01-01T00:00:00Z",
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


