#!/usr/bin/env python

import os, sys, re, socket

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, getLogger, cp_get, cp_getBoolean
from gip_storage import connect_admin, getSESpace, getSEVersion, getSETape, \
    seHasTape, voListStorage

log = getLogger("GIP.services_info_provider")

def print_se(cp):
    """
    Print out the GLUE storage element entity for this site.

    Config values used:
       * site.unique_name: The associated site's unique name.  Required.
       * site.name: The associated site's human-readable name.  Required.
       * bdii.endpoint: The associated endpoint.  Required
       * se.name: The storage element's human-readable name.
       * se.unique_name: The storage element's human-readable name.

    @param cp: GIP configuration object.
    """
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
    """
    Print out the access protocol information for the dCache storage element
    
    This utilizes the LoginBroker cell of dCache to provide us with info.

    Config elements used:
       * se.unique_name: The unique name of the 

    @param admin: A dCacheAdmin.Admin object; the interface to the dCache admin.
    @param cp: The GIP configuration object.
    """
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
    """
    Print out the dCache's SRM servers using the admin interface

    Config parameters directly used:
       * se.unique_name: The associated SE's unique name.  Must be present
       * site.unique_name: The associated site's unique name.  Must be present
    """
    sename = cp.get("se", "unique_name")
    sitename = cp.get("site", "unique_name")
    #vos = [i.strip() for i in cp.get("vo", "vos").split(',')]
    ServiceTemplate = getTemplate("GlueService", "GlueServiceUniqueID")
    ControlTemplate = getTemplate("GlueSE", "GlueSEControlProtocolLocalID")

    # Determine the VOs which are allowed to use this storage element
    # TODO: not GLUE v2.0 safe
    acbr_tmpl = '\nGlueServiceAccessControlRule: %s' \
                '\nGlueServiceAccessControlRule: VO:%s'
    acbr = ''
    vos = voListStorage(cp)
    for vo in vos:
        acbr += acbr_tmpl % (vo, vo)

    # Use the srm-LoginBroker cell to list all the SRM cells available.
    results = admin.execute("srm-LoginBroker", "ls")
    for line in results.split("\n"):
        if len(line.strip()) == 0:
            continue
        #Lines have the following format:
        #SRM-srm@srm-srmDomain;Storage;{SRM,1.1.1};[srm:8443];<0,300000>;
        doorname, kind, versions, host, logins, dummy = line.split(';')
        protocol, version = versions[1:-1].split(',')
        hostname, port = host[1:-1].split(':')
        hostname = hostname.split(',')[0]
        # Make sure we have a FQDN (dCache has a nasty habit of dropping the
        # domain name internally.
        try:
            hostname = socket.getfqdn(hostname)
        except:
            pass

        # From the SRM info, build the information for the GLUE entity.
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

        # Augment the information with SRM v1 protocol information, then
        # print out the control and service entries
        info['version'] = "1.1.0"
        endpoint = "httpg://%s:%i/srm/managerv1" % (hostname, int(port))
        info['endpoint'] = endpoint
        info['serviceID'] = endpoint
        info['uri'] = endpoint
        info['url'] = endpoint
        info['serviceName'] = endpoint
        # Bugfix: Make the control protocol unique ID unique between the SRM
        # versions
        info['cpLocalID'] = doorname + '_srmv1'
        print ControlTemplate % info
        print ServiceTemplate % info

        # Change the v1 information to v2 and print out again
        info['version'] = "2.2.0"
        endpoint = "httpg://%s:%i/srm/managerv2" % (hostname, int(port))
        info['endpoint'] = endpoint
        info['serviceID'] = endpoint
        info['uri'] = endpoint
        info['url'] = endpoint
        info['serviceName'] = endpoint
        info["wsdl"] = "http://sdm.lbl.gov/srm-wg/srm.v2.2.wsdl"
        info["semantics"] = "http://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.pdf"
        # Bugfix: Make the control protocol unique ID unique between the SRM
        # versions
        info['cpLocalID'] = doorname + '_srmv2'
        print ControlTemplate % info
        print ServiceTemplate % info

def print_srm_compat(cp):
    """
    In the case of an error for the dynamic stuff, advertise a down SRM.

    Use the static data present in the GIP to show the storage element.  The
    used config parameters are:

       * se.srm_present: set to True to advertise the SRM element in fallback.
          Default is True
       * se.publish_down: Set to False to advertise the status as UNKNOWN
          instead of CRITICAL.  Default is True
       * se.srm_host: Set to the SRM hostname.  Default is UNAVAILABLE
       * se.srm_version: Set to the SRM protocol version.  Default is 2.2.0
       * se.unique_name: The unique name of the storage element
       * site.unique_name: The unique name of the associated site.

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
    """
    Print the information about a dCache storage element.  This script prints
    out the access/control protocols as well as the SRM storage element

    In the case that something bad happens, we fall back to printing out the
    SRM service based on static data.
    """
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

