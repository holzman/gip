
from gip_common import cp_get, getLogger, config, getTemplate, printTemplate, \
    cp_getBoolean, cp_getInt
from gip_storage import voListStorage, getPath, getSESpace, getSETape, seHasTape

log = getLogger("GIP.Storage.Generic")

def print_SA(cp):
    """
    Print out the SALocal information for GLUE 1.3.
    """ 
    vos = voListStorage(cp)
    se_unique_id = cp.get("se", "unique_name")
    se_name = cp.get("se", "name")
    saTemplate = getTemplate("GlueSE", "GlueSALocalID")
    try:
        used, available = getSESpace(cp)
    except Exception, e:
        log.error("Unable to get SE space: %s" % str(e))
        used = 0
        available = 0
    for vo in vos:
        acbr = "GlueSAAccessControlBaseRule: VO:%s" % vo
        info = {"saLocalID"        : vo,
                "seUniqueID"       : se_unique_id,
                "root"             : "/",
                "path"             : getPath(cp, vo),
                "filetype"         : "permanent", 
                "saName"           : "%s_default" % vo,
                "totalOnline"      : 0,
                "usedOnline"       : 0,
                "freeOnline"       : 0,
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
        print printTemplate(saTemplate, info)

def print_SE(cp):
    status = cp_get(cp, "se", "status", "Production")
    version = cp_get(cp, "se", "version", "UNKNOWN")
    try:
        used, available, total = getSESpace(cp)
    except:
        used, available, total = 0, 0, 0
    nu, nf, nt = getSETape(cp)
    bdiiEndpoint = cp.get("bdii", "endpoint")
    siteUniqueID = cp.get("site", "unique_name")
    implementation = cp_get(cp, "se", "implementation", "UNKNOWN")
    if seHasTape(cp):
        arch = "tape"
    elif implementation=='dcache' or implementation.lower() == 'bestman/xrootd':
        arch = 'multi-disk'
    else:
        arch = 'disk'
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
    seTemplate = getTemplate("GlueSE", "GlueSEUniqueID")
    printTemplate(seTemplate, info)

def print_SRM(cp):
    if not cp_getBoolean(cp, "se", "srm_present", True):
        return

    sename = cp.get("se", "unique_name")
    sitename = cp.get("site", "unique_name")
    srmname = cp.get("se", "srm_host")

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
            "cpLocalID"    : srmname,
            "seUniqueID"   : sename,
            "protocolType" : "SRM",
            "capability"   : "control",
            "status"       : "OK",
            "statusInfo"   : "SRM instance untested.",
            "wsdl"         : "http://sdm.lbl.gov/srm-wg/srm.v1.1.wsdl",
            "semantics"    : "http://sdm.lbl.gov/srm-wg/doc/srm.v1.0.pdf",
            "startTime"    : "1970-01-01T00:00:00Z",
        }

    # Originally, there was a SRMv1 block before this... no longer needed
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
    printTemplate(ControlTemplate, info)
    printTemplate(ServiceTemplate, info)


def print_access(cp):
    raise NotImplementedError()

def main():
    cp = config()
    print_SE(cp)
    print_SA(cp)
    print_SRM(cp)
    print_access(cp)

