#!/usr/bin/env python

import os, sys, re, socket

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger
from gip_storage import connect_admin, getSESpace, getSEVersion, getSETape, \
    seHasTape, voListStorage

log = getLogger("GIP.services_info_provider")

def print_se(cp):
    try:
        admin = connect_admin(cp)
        status = "Production"
    except Exception, e:
        log.error("Error occurred when connecting to the admin interface: %s" % \
                  str(e))
        status = "Closed"
    if status == "Production":
        try:
            used, available, total = getSESpace(cp, admin, total=True, gb=True)
        except Exception, e:
            log.error("Error occurred when querying the total space: %s" % \
                      str(e))
            used, available, total = 0, 0, 0
        try:
            version = getSEVersion(cp, admin)
        except Exception, e:
            log.error("Error occurred when querying the version number: %s" % \
                      str(e))
            version = "UNKNOWN"
    else:
        used = 0
        available = 0
        total = 0
        version = "UNKNOWN"
    seTemplate = getTemplate("GlueSE", "GlueSEUniqueID")
    if seHasTape(cp):
        arch = "tape"
    else:
        arch = "multi-disk"
    nu, nf, nt = getSETape(cp) 
    siteUniqueID = cp.get("site", "unique_name")
    siteName = cp.get("site", "name")
    bdiiEndpoint = cp.get("bdii", "endpoint") + ("/mds-vo-name=%s," \
        "mds-vo-name=local,o=grid" % siteName)
    info = { 'seName'         : cp.get("se", "name"),
             'seUniqueID'     : cp.get("se", "unique_name"),
             'implementation' : 'dcache',
             "version"        : version,
             "status"         : status,
             "port"           : 8443,
             "onlineTotal"    : total,
             "nearlineTotal"  : nt,
             "onlineUsed"     : used,
             "nearlineUsed"   : nu,
             "architecture"   : arch,
             "free"           : available,
             "total"          : total,
             "bdiiEndpoint"   : bdiiEndpoint,
             "siteUniqueID"   : siteUniqueID,
             "arch"           : arch,
           }
    print seTemplate % info

def print_access_protocols(cp, admin):
    sename = cp.get("se", "unique_name")
    results = admin.execute("LoginBroker", "ls")
    accessTemplate = getTemplate("GlueSE", "GlueSEAccessProtocolLocalID")
    for line in results.split('\n'):
        if len(line.strip()) == 0:
            continue
        doorname, kind, versions, host, logins, dummy = line.split(';')
        protocol, version = versions[1:-1].split(',')
        hostname, port = host[1:-1].split(':')
        hostname = hostname.split(',')[0]
        try:
            hostname = socket.getfqdn(hostname)
        except:
            pass
        endpoint = "%s://%s:%i" % (protocol, hostname, int(port))
        securityinfo = ''
        if protocol == 'gsiftp':
            securityinfo = "gsiftp"
        elif protocol == 'dcap':
            securityinfo = "none"
        else:
            securityinfo = "none"
        info = {'accessProtocolID': doorname,
                'seUniqueID'      : sename,
                'protocol'        : protocol,
                'endpoint'        : endpoint,
                'capability'      : 'file transfer',
                'maxStreams'      : 10,
                'security'        : securityinfo,
                'port'            : port,
                'version'         : version,
               }
        print accessTemplate % info

def print_srm(cp, admin):
    sename = cp.get("se", "unique_name")
    sitename = cp.get("site", "unique_name")
    #vos = [i.strip() for i in cp.get("vo", "vos").split(',')]
    vos = voListStorage(cp)
    ServiceTemplate = getTemplate("GlueService", "GlueServiceUniqueID")
    ControlTemplate = getTemplate("GlueSE", "GlueSEControlProtocolLocalID")
    acbr_tmpl = '\nGlueServiceAccessControlRule: %s' \
                '\nGlueServiceAccessControlRule: VO:%s'
    acbr = ''
    for vo in vos:
        acbr += acbr_tmpl % (vo, vo)
    results = admin.execute("srm-LoginBroker", "ls")
    for line in results.split("\n"):
        if len(line.strip()) == 0:
            continue
        doorname, kind, versions, host, logins, dummy = line.split(';')
        protocol, version = versions[1:-1].split(',')
        hostname, port = host[1:-1].split(':')
        hostname = hostname.split(',')[0]
        try:
            hostname = socket.getfqdn(hostname)
        except:
            pass
        info = {
                "serviceType"  : "SRM",
                "acbr"         : acbr[1:],
                "siteID"       : sitename,
                "cpLocalID"    : doorname,
                "seUniqueID"   : sename,
                "protocolType" : "SRM",
                "capability"   : "control",
                "status"       : "Production",
                "statusInfo"   : "UNKNOWN",
                "wsdl"         : "http://sdm.lbl.gov/srm-wg/srm.v1.1.wsdl",
                "semantics"    : "http://sdm.lbl.gov/srm-wg/doc/srm.v1.0.pdf",
                "startTime"    : "1970-01-01T00:00:00Z",
               }

        info['version'] = "1.1"
        endpoint = "httpg://%s:%i/srm/managerv1" % (hostname, int(port))
        info['endpoint'] = endpoint
        info['serviceID'] = endpoint
        info['uri'] = endpoint
        info['url'] = endpoint
        info['serviceName'] = endpoint
        print ControlTemplate % info
        print ServiceTemplate % info

        info['version'] = "2.2"
        endpoint = "httpg://%s:%i/srm/managerv2" % (hostname, int(port))
        info['endpoint'] = endpoint
        info['serviceID'] = endpoint
        info['uri'] = endpoint
        info['url'] = endpoint
        info['serviceName'] = endpoint
        info["wsdl"] = "http://sdm.lbl.gov/srm-wg/srm.v2.2.wsdl"
        info["semantics"] = "http://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.pdf"
        print ControlTemplate % info
        print ServiceTemplate % info

def main():
    try:
        cp = config("$GIP_LOCATION/etc/dcache_storage.conf", \
            "$GIP_LOCATION/etc/dcache_password.conf", \
            "$GIP_LOCATION/etc/tape_info.conf")
        print_se(cp)
        admin = connect_admin(cp)
        print_access_protocols(cp, admin)
        print_srm(cp, admin)
    except:
        # Make sure we don't feed the error to the BDII stream;
        # fail silently, hopefully someone logs the stderr.
        sys.stdout = sys.stderr
        raise

if __name__ == '__main__':
    main()

