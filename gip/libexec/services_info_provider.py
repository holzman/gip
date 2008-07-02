#!/usr/bin/env python

import os, sys, re, socket

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger, cp_get, cp_getBoolean
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
    # BUGFIX: Resolve the IP address of srm host that the admin specifies.
    # If this IP address matches the IP address given by dCache, then we will
    # print out the admin-specified hostname instead of looking it up.  This
    # is for sites where the SRM host is a CNAME instead of the A name.
    srm_host = cp_get(cp, "se", "srm_host", None)
    if srm_host:
        try:
            srm_ip = socket.gethostbyname(srm_host)
        except:
            srm_ip = None
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
            hostname_ip = socket.gethostbyname(hostname)
        except:
            hostname_ip = None
        if hostname_ip != None and hostname_ip == srm_ip and srm_host != None:
            hostname = srm_host
        info = {
                "serviceType"  : "SRM",
                "acbr"         : acbr[1:],
                "siteID"       : sitename,
                "cpLocalID"    : doorname,
                "seUniqueID"   : sename,
                "protocolType" : "SRM",
                "capability"   : "control",
                "status"       : "OK",
                "statusInfo"   : "SRM instance is responding.",
                "wsdl"         : "http://sdm.lbl.gov/srm-wg/srm.v1.1.wsdl",
                "semantics"    : "http://sdm.lbl.gov/srm-wg/doc/srm.v1.0.pdf",
                "startTime"    : "1970-01-01T00:00:00Z",
               }

        info['version'] = "1.1.0"
        endpoint = "httpg://%s:%i/srm/managerv1" % (hostname, int(port))
        info['endpoint'] = endpoint
        info['serviceID'] = endpoint
        info['uri'] = endpoint
        info['url'] = endpoint
        info['serviceName'] = endpoint
        info['cpLocalID'] = doorname + '_srmv1'
        print ControlTemplate % info
        print ServiceTemplate % info

        info['version'] = "2.2.0"
        endpoint = "httpg://%s:%i/srm/managerv2" % (hostname, int(port))
        info['endpoint'] = endpoint
        info['serviceID'] = endpoint
        info['uri'] = endpoint
        info['url'] = endpoint
        info['serviceName'] = endpoint
        info["wsdl"] = "http://sdm.lbl.gov/srm-wg/srm.v2.2.wsdl"
        info["semantics"] = "http://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.pdf"
        info['cpLocalID'] = doorname + '_srmv2'
        print ControlTemplate % info
        print ServiceTemplate % info

def print_srm_compat(cp):
    """
    In the case of an error for the dynamic stuff, advertise a down SRM.
    """
    if cp_get(cp, "se", "srm_present", "n").lower().find("n") >= 0 or \
            cp_get(cp, "se", "srm_present", "n").lower().find("f") >= 0 or \
            cp_get(cp, "se", "srm_present", "n") == "0":
        return
    publish_down = cp_getBoolean(cp, "se", "publish_down", True)
    srm_host = cp_get(cp, "se", "srm_host", "UNAVAILABLE")
    srm_version = cp_get(cp, "se", "srm_version", "2.2.0")
    port = 8443
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
    
    # Maybe dynamic dCache stuff is just broken, and we always want to
    # publish things as in production
    if publish_down:
        status = "Critical"
    else:
        status = "Unknown"
    info = {
        "serviceType"  : "SRM",
        "acbr"         : acbr[1:],
        "siteID"       : sitename,
        "cpLocalID"    : srm_host,
        "seUniqueID"   : sename,
        "protocolType" : "SRM",
        "capability"   : "control",
        "status"       : status,
        "statusInfo"   : "An error occurred when looking up the SRM info.",
        "wsdl"         : "http://sdm.lbl.gov/srm-wg/srm.v1.1.wsdl",
        "semantics"    : "http://sdm.lbl.gov/srm-wg/doc/srm.v1.0.pdf",
        "startTime"    : "1970-01-01T00:00:00Z",
    }

    info['version'] = srm_version
    if srm_version.find('1') >= 0:
        endpoint = "httpg://%s:%i/srm/managerv1" % (srm_host, int(port))
    else:
        endpoint = "httpg://%s:%i/srm/managerv2" % (srm_host, int(port))
    info['endpoint'] = endpoint
    info['serviceID'] = endpoint
    info['uri'] = endpoint
    info['url'] = endpoint
    info['serviceName'] = endpoint
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
    except Exception, e:
        # Make sure we don't feed the error to the BDII stream;
        # fail silently, hopefully someone logs the stderr.
        print_srm_compat(cp)
        sys.stdout = sys.stderr
        log.exception(e)
        #raise

if __name__ == '__main__':
    main()

