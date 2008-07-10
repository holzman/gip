
from gip_common import cp_get, getLogger, config, getTemplate, printTemplate, \
    cp_getBoolean, cp_getInt
from gip_storage import voListStorage, getPath, getSESpace, getSETape, \
    seHasTape, getAccessProtocols

log = getLogger("GIP.Storage.Generic")

def print_SA(cp, section="se"):
    """
    Print out the SALocal information for GLUE 1.3.
    """ 
    vos = voListStorage(cp)
    se_unique_id = cp.get(section, "unique_name")
    se_name = cp.get(section, "name")
    saTemplate = getTemplate("GlueSE", "GlueSALocalID")
    try:
        if section == 'se':
            used, available, total = getSESpace(cp, total=True)
        elif section == 'classic_se':
            used, available, total = getClassicSESpace(cp, total=True)
        else:
            raise Exception("Unknown SE type!")
    except Exception, e:
        log.error("Unable to get SE space: %s" % str(e))
        used = 0
        available = 0
        total = 0
    for vo in vos:
        acbr = "GlueSAAccessControlBaseRule: VO:%s" % vo
        info = {"saLocalID"        : vo,
                "seUniqueID"       : se_unique_id,
                "root"             : "/",
                "path"             : getPath(cp, vo, section=section),
                "filetype"         : "permanent", 
                "saName"           : "%s_default" % vo,
                "totalOnline"      : int(round(total/1000**2)),
                "usedOnline"       : int(round(used/1000**2)),
                "freeOnline"       : int(round(available/1000**2)),
                "reservedOnline"   : 0,
                "totalNearline"    : 0,
                "usedNearline"     : 0,
                "freeNearline"     : 0,
                "reservedNearline" : 0,
                "retention"        : "replica",
                "accessLatency"    : "online",
                "expiration"       : "neverExpire",
                "availableSpace"   : available,
                "usedSpace"        : used,
                "acbr"             : acbr,
               }
        printTemplate(saTemplate, info)

def print_classicSE(cp):

    if not cp_getBoolean(cp, "classic_se", "advertise_se", False):
        log.info("Not advertising a classic SE.")
        return
    if not cp_getBoolean(cp, "se", "shares_fs_with_ce", False):
        log.info("Not advertising a classic SE because the SE shares a FS.")
        return

    status = cp_get(cp, "se", "status", "Production")
    version = cp_get(cp, "se", "version", "UNKNOWN")
    try:
        used, available, total = getClassicSESpace(cp, total=True)
    except:
        used, available, total = 0, 0, 0

    # Tape information, if we have it...
    nu, nf, nt = getSETape(cp)

    bdiiEndpoint = cp.get("bdii", "endpoint")
    siteUniqueID = cp.get(cp, "site", "unique_name")
    implementation = cp_get(cp, "classic_se", "implementation", "classicSE")
    arch = 'other'

    # Fill in the information for the template
    fallback_name = siteUniqueID + "_classicSE"
    seName = cp_get(cp, "classic_se", "name", fallback_name)
    seUniqueID = cp_get(cp, "classic_se", "unique_name", fallback_name)
    info = { 'seName'         : seName,
             'seUniqueID'     : seUniqueID,
             'implementation' : implementation,
             "version"        : version,
             "status"         : status,
             "port"           : 2811,
             "onlineTotal"    : 0,
             "nearlineTotal"  : 0,
             "onlineUsed"     : used,
             "nearlineUsed"   : nu,
             "architecture"   : arch,
             "free"           : available,
             "total"          : total,
             "bdiiEndpoint"   : bdiiEndpoint,
             "siteUniqueID"   : siteUniqueID,
             "arch"           : arch,
           }
    seTemplate = getTemplate("GlueSE", "GlueSEUniqueID")
    printTemplate(seTemplate, info)

    print_SA(cp, section="classic_se")
    print_classic_access(cp)

def print_SE(cp):
    status = cp_get(cp, "se", "status", "Production")
    version = cp_get(cp, "se", "version", "UNKNOWN")

    # Determine space information
    try:
        used, available, total = getSESpace(cp, total=True)
    except:
        used, available, total = 0, 0, 0

    # Tape information, if we have it...
    nu, nf, nt = getSETape(cp)

    bdiiEndpoint = cp.get("bdii", "endpoint")
    siteUniqueID = cp.get("site", "unique_name")
    implementation = cp_get(cp, "se", "implementation", "UNKNOWN")

    # Try to guess the appropriate architecture
    if seHasTape(cp):
        arch = "tape"
    elif implementation=='dcache' or implementation.lower() == 'bestman/xrootd':
        arch = 'multi-disk'
    elif implementation.lower() == 'bestman':
        arch = 'disk'
    else:
        arch = 'other'

    # Fill in the information for the template
    info = { 'seName'         : cp.get("se", "name"),
             'seUniqueID'     : cp.get("se", "unique_name"),
             'implementation' : implementation,
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
    seTemplate = getTemplate("GlueSE", "GlueSEUniqueID")
    printTemplate(seTemplate, info)

    print_SA(cp)
    print_access(cp)

def print_SRM(cp):
    if not cp_getBoolean(cp, "se", "srm_present", True):
        return

    sename = cp.get("se", "unique_name")
    sitename = cp.get("site", "unique_name")
    srmname = cp.get("se", "srm_host")

    srm_version = cp_get(cp, "se", "srm_version", "2")
    port = cp_getInt(cp, "se", "srm_port", 8443)
    endpoint = cp_get(cp,"se", "srm_endpoint", "httpg://%s:%i/srm/managerv2" % \
        (srmname, int(port)))

    ServiceTemplate = getTemplate("GlueService", "GlueServiceUniqueID")
    ControlTemplate = getTemplate("GlueSE", "GlueSEControlProtocolLocalID")

    # Determine the VOs which are allowed to use this storage element
    # TODO: not GLUE v2.0 safe
    acbr_tmpl = '\nGlueServiceAccessControlRule: VO:%s'
                
    acbr = ''
    vos = voListStorage(cp)
    for vo in vos:
        acbr += acbr_tmpl % vo

    info = {
            "serviceType"  : "SRM",
            "acbr"         : acbr[1:],
            "siteID"       : sitename,
            "seUniqueID"   : sename,
            "protocolType" : "SRM",
            "capability"   : "control",
            "status"       : "OK",
            "statusInfo"   : "SRM instance untested.",
            "wsdl"         : "http://sdm.lbl.gov/srm-wg/srm.v1.1.wsdl",
            "semantics"    : "http://sdm.lbl.gov/srm-wg/doc/srm.v1.0.pdf",
            "startTime"    : "1970-01-01T00:00:00Z",
        }

    if srm_version.find('2') >= 0:
        info['version'] = "2.2.0"
        info['endpoint'] = endpoint
        info['serviceID'] = endpoint
        info['uri'] = endpoint
        info['url'] = endpoint
        info['serviceName'] = endpoint
        info["wsdl"] = "http://sdm.lbl.gov/srm-wg/srm.v2.2.wsdl"
        info["semantics"] = "http://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.pdf"
        # Bugfix: Make the control protocol unique ID unique between the SRM
        # versions
        info['cpLocalID'] = srmname + '_srmv2'
    else:
        info['version'] = '1.1.0'
        info['endpoint'] = endpoint
        info['serviceID'] = endpoint
        info['uri'] = endpoint
        info['url'] = endpoint
        info['serviceName'] = endpoint
        info['cpLocalID'] = srmname + '_srmv1'

    printTemplate(ControlTemplate, info)
    printTemplate(ServiceTemplate, info)


def print_access(cp):
    sename = cp.get("se", "unique_name")
    accessTemplate = getTemplate("GlueSE", "GlueSEAccessProtocolLocalID")

    for info in getAccessProtocols(cp):
        if 'endpoint' not in info:
            info['endpoint'] = "%s://%s:%i"%(info['protocol'], info['hostname'],
                int(info['port']))
        if 'securityinfo' not in info:
            if protocol == 'gsiftp':
                securityinfo = "gsiftp"
            elif protocol == 'dcap' or protocol[:5] == 'xroot':
                securityinfo = "none"
            else:
                securityinfo = "none"
            info['securityinfo'] = securityinfo
        info['accessProtocolID'] = info['protocol'].upper() + "_" + \
                                   info['hostname'] + "_" + info['port']
        info['seUniqueID'] = sename
        if 'capability' not in info:
            info['capability'] = 'file transfer'
        if 'maxStreams' not in info:
            info['maxStreams'] = 1,
        if 'version' not in info:
            info['version'] = 'UNKNOWN',
        print accessTemplate % info

def print_classic_access(cp):
    fallback_name = siteUniqueID + "_classicSE"
    seName = cp_get(cp, "classic_se", "name", fallback_name)
    seUniqueID = cp_get(cp, "classic_se", "unique_name", fallback_name)
    host = cp_get(cp, "classic_se", "host")
    port = cp_getInt(cp, "classic_se", "port", "2811")
    accessTemplate = getTemplate("GlueSE", "GlueSEAccessProtocolLocalID")

    endpoint = 'gsiftp://%s:%i' % (host, port)

    info = {'accessProtocolID' : 'GFTP_%s_%i' % (host, port),
            'seUniqueID'       : seUniqueID,
            'protocol'         : 'gsiftp',
            'endpoint'         : endpoint,
            'capability'      : 'file transfer',
            'maxStreams'      : 10,
            'security'        : 'gsiftp',
            'port'            : port,
            'version'         : '1.0.0',
           }
    print accessTemplate % info

def main():
    cp = config()
    print_SE(cp)
    print_classicSE(cp)
    print_SRM(cp)

