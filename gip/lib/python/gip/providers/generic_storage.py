
"""
A generic provider for storage elements; written for the StorageElement class 
in gip_storage.
"""

import socket

from gip_common import cp_get, getLogger, config, getTemplate, printTemplate, \
    cp_getBoolean, cp_getInt
from gip_storage import voListStorage, getSETape, \
    getClassicSESpace, StorageElement
from gip.bestman.BestmanInfo import BestmanInfo
from gip.dcache.DCacheInfo import DCacheInfo

log = getLogger("GIP.Storage.Generic")

def print_SA(se, cp, section="se"): #pylint: disable-msg=W0613
    """
    Print out the SALocal information for GLUE 1.3.
    """ 
    for sa in se.getSAs():
        try:
            print_single_SA(sa, se, cp)
        except Exception, e:
            log.exception(e)

def print_single_SA(info, se, cp): #pylint: disable-msg=W0613
    """
    Print out the GLUE for a single SA.
    """
    se_unique_id = se.getUniqueID()
    saTemplate = getTemplate("GlueSE", "GlueSALocalID")

    info.setdefault('seUniqueID', se_unique_id)
    info.setdefault('saLocalID', 'UNKNOWN_SA')
    info.setdefault('root', '/')
    info.setdefault('path', '/UNKNOWN')
    info.setdefault('filetype', 'permanent')
    info.setdefault('saName', info['saLocalID'])
    info.setdefault('totalOnline', 0)
    info.setdefault('usedOnline', 0)
    info.setdefault('freeOnline', 0)
    info.setdefault('reservedOnline', 0)
    info.setdefault('totalNearline', 0)
    info.setdefault('usedNearline', 0)
    info.setdefault('freeNearline', 0)
    info.setdefault('reservedNearline', 0)
    info.setdefault('retention', 'replica')
    info.setdefault('accessLatency', 'online')
    info.setdefault('expiration', 'neverExpire')
    info.setdefault('availableSpace', 0)
    info.setdefault('usedSpace', 0)
    printTemplate(saTemplate, info)

def print_VOInfo(se, cp):
    """
    Print out the VOInfo GLUE information for all the VOInfo
    objects in the SE.
    """
    for voinfo in se.getVOInfos():
        try:
            print_single_VOInfo(voinfo, se, cp)
        except Exception, e:
            log.exception(e)

def print_single_VOInfo(voinfo, se, cp): #pylint: disable-msg=W0613
    """
    Emit the GLUE entity for a single VOInfo dictionary.
    """
    voinfoTemplate = getTemplate('GlueSE', 'GlueVOInfoLocalID')
    voinfo.setdefault('acbr', 'GlueVOInfoAccessControlBaseRule: UNKNOWN')
    voinfo.setdefault('path', '/UNKNOWN')
    voinfo.setdefault('tag', 'Not A Space Reservation')
    voinfo.setdefault('seUniqueID', se.getUniqueID())
    printTemplate(voinfoTemplate, voinfo)

def print_classicSE(cp):
    """
    Emit the relevant GLUE entities for a ClassicSE.
    """
    if not cp_getBoolean(cp, "classic_se", "advertise_se", False):
        log.info("Not advertising a classic SE.")
        return
    else:
        log.info("Advertising a classic SE.")
    if cp_getBoolean(cp, "se", "shares_fs_with_ce", False):
        log.info("Not advertising a classic SE because the SE shares a FS.")
        return

    status = cp_get(cp, "se", "status", "Production")
    version = cp_get(cp, "se", "version", "UNKNOWN")
    try:
        used, available, total = getClassicSESpace(cp, total=True, gb=True)
    except Exception, e:
        log.error("Unable to get SE space: %s" % str(e))
        used, available, total = 0, 0, 0

    # Tape information, if we have it...
    nu, _, nt = getSETape(cp)

    bdiiEndpoint = cp.get("bdii", "endpoint")
    siteUniqueID = cp.get("site", "unique_name")
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

    vos = voListStorage(cp)
    try:        
        used, available, total = getClassicSESpace(cp, total=True)
    except Exception, e:
        used = 0
        available = 0
        total = 0
    acbr = []
    for vo in vos:
        acbr.append("GlueSAAccessControlBaseRule: VO:%s" % vo)
    acbr = '\n'.join(acbr)
    path = cp_get(cp, "osg_dirs", "data", "/UNKNOWN")
    info = {"saLocalID"        : seUniqueID,
            "seUniqueID"       : seUniqueID,
            "root"             : "/",
            "path"             : path,
            "filetype"         : "permanent",
            "saName"           : seUniqueID,
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
    saTemplate = getTemplate("GlueSE", "GlueSALocalID")
    printTemplate(saTemplate, info)

    print_classic_access(cp, siteUniqueID)

def print_SE(se, cp):
    """
    Emit the GLUE entities for the SE, based upon the StorageElement class.
    """
    status = se.getStatus()
    version = se.getVersion()

    # Determine space information
    try:
        used, available, total = se.getSESpace(total=True, gb=True)
    except:
        used, available, total = 0, 0, 0

    # Tape information, if we have it...
    nu, _, nt = se.getSETape()

    bdiiEndpoint = cp.get("bdii", "endpoint")
    siteUniqueID = cp.get("site", "unique_name")
    implementation = se.getImplementation()

    # Try to guess the appropriate architecture
    arch = se.getSEArch()

    # Fill in the information for the template
    info = { 'seName'         : se.getName(),
             'seUniqueID'     : se.getUniqueID(),
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

    try:
        print_SA(se, cp)
    except Exception, e:
        log.exception(e)
    try:
        print_VOInfo(se, cp)
    except Exception, e:
        log.exception(e)

    try:
        print_access(se, cp)
    except Exception, e:
        log.exception(e)

def print_SRM(se, cp):
    """
    Print out the GLUE service and control protocol entities for the
    SRM endpoints in the SE.
    """
    if not se.hasSRM():
        return

    for info in se.getSRMs():
        try:
            print_single_SRM(info, se, cp)
        except:
            pass

def print_single_SRM(info, se, cp):
    """
    Print out the GLUE service and CP entities for a single SRM dictionary.
    """
    sitename = cp.get("site", "unique_name")
    sename = se.getUniqueID()
    version = info.setdefault('version', '2.2.0')
    info.setdefault('siteID', sitename)
    info.setdefault('seUniqueID', sename)
    info.setdefault('startTime', '1970-01-01T00:00:00Z')
    endpoint = info.get('endpoint', 'httpg://example.org:8443/srm/managerv2')
    if version.find('2') >= 0:
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
        info['cpLocalID'] = info.get('name', sename) + '_srmv2'
    else:
        info['version'] = '1.1.0'
        info['endpoint'] = endpoint
        info['serviceID'] = endpoint
        info['uri'] = endpoint
        info['url'] = endpoint
        info['serviceName'] = endpoint
        info['cpLocalID'] = info.get('name', sename) + '_srmv1'

    ServiceTemplate = getTemplate("GlueService", "GlueServiceUniqueID")
    ControlTemplate = getTemplate("GlueSE", "GlueSEControlProtocolLocalID")

    printTemplate(ControlTemplate, info)
    printTemplate(ServiceTemplate, info)


def print_access(se, cp): #pylint: disable-msg=W0613
    """
    Emit the GLUE entities for a StorageElement's access protocols.
    """
    sename = se.getUniqueID()
    accessTemplate = getTemplate("GlueSE", "GlueSEAccessProtocolLocalID")

    for info in se.getAccessProtocols():
        protocol = info.setdefault('protocol', 'gsiftp')
        if 'endpoint' not in info:
            info['endpoint'] = "%s://%s:%i"% (info['protocol'], 
                                              info['hostname'],
                                              int(info['port']))
        if 'securityinfo' not in info:
            if protocol == 'gsiftp':
                securityinfo = "gsiftp"
            elif protocol == 'dcap' or protocol[:5] == 'xroot':
                securityinfo = "none"
            else:
                securityinfo = "none"
            info['securityinfo'] = securityinfo
        info.setdefault('security', 'GSI')
        info['accessProtocolID'] = info['protocol'].upper() + "_" + \
                                   info['hostname'] + "_" + info['port']
        info['seUniqueID'] = sename
        if 'capability' not in info:
            info['capability'] = 'file transfer'
        if 'maxStreams' not in info:
            info['maxStreams'] = 1
        if 'version' not in info:
            info['version'] = 'UNKNOWN',
        print accessTemplate % info

def print_classic_access(cp, siteUniqueID):
    """
    Emit the GLUE entity for a classic SE's access protocol.
    """
    fallback_name = siteUniqueID + "_classicSE"
    seUniqueID = cp_get(cp, "classic_se", "unique_name", fallback_name)
    try:
        default_host = socket.gethostname()
    except:
        default_host = 'UNKNOWN.example.org'
    host = cp_get(cp, "classic_se", "host", default_host)
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
    """
    The primary wrapper function for emitting GLUE for storage elements.
    """
    cp = config()
    impl = cp_get(cp, "se", "implementation", "UNKNOWN")
    if impl.lower().find('bestman') >= 0:
        se = BestmanInfo(cp)
    elif impl.lower().find('dcache') >= 0:
        cp = config("$GIP_LOCATION/etc/dcache_storage.conf")
        se = DCacheInfo(cp)
    else:
        se = StorageElement(cp)
    try:
        se.run()
    except Exception, e:
        log.exception(e)
    print_SE(se, cp)
    try:
        print_classicSE(cp)
    except Exception, e:
        log.exception(e)
    try:
        print_SRM(se, cp)
    except Exception, e:
        log.exception(e)
        
if __name__ == '__main__':
    main()
