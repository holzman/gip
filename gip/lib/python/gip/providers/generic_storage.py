
"""
A generic provider for storage elements; written for the StorageElement class
in gip_storage.
"""

import os
import sys
import time
import socket

from gip_common import cp_get, getLogger, config, getTemplate, printTemplate, \
    cp_getBoolean, cp_getInt, normalizeFQAN
from gip_storage import voListStorage, getSETape, \
    getClassicSESpace, StorageElement
from gip.bestman.BestmanInfo import BestmanInfo
from gip.dcache.DCacheInfo import DCacheInfo
from gip.dcache.DCacheInfo19 import DCacheInfo19
import gip_sets as sets

log = getLogger("GIP.Storage.Generic")

# Try to load up the Gratia StorageElement and StorageElementRecord modules.
# If successful, the GIP has the capability to send information to Gratia.
# The information we can send to Gratia is ultimately above and beyond the info
# which we can fit in the BDII schema.
has_gratia_capacity = True
try:
    # Try hard to bootstrap paths.
    paths = ['/opt/vdt/gratia/probe/common', '$VDT_LOCATION/gratia/probe/'\
        'common', '/opt/vdt/gratia/probe/service', '$VDT_LOCATION/gratia/probe'\
        '/service']
    for path in paths:
        path = os.path.expandvars(path)
        if os.path.exists(path) and path not in sys.path:
            sys.path.append(path)

    # Try to import the necessary Gratia modules.
    import Gratia
    import StorageElement
    import StorageElementRecord
except:
    has_gratia_capacity = False
    log.warning("Could not import the Gratia StorageElement modules.")

def print_SA(se, cp, section="se"): #pylint: disable-msg=W0613
    """
    Print out the SALocal information for GLUE 1.3.
    """
    vo_limit_str = cp_get(cp, section, "vo_limits", "")
    vo_limit = {}
    cumulative_total = {}
    # Determine the limits for each VO
    for vo_str in vo_limit_str.split(','):
        vo_str = vo_str.strip()
        info = vo_str.split(":")
        if len(info) != 2:
            continue
        vo = info[0].strip()
        try:
            limit = float(info[1].strip())
        except:
            continue
        vo_limit[vo] = limit
        cumulative_total.setdefault(vo, 0)
    for sa in se.getSAs():
        do_continue = False
        reduce_by_amount = 0
        # Calculate the total online amount
        try:
            totalOnline = float(sa.get("totalOnline", 0))
        except:
            continue

        # If the ACBR is blank, don't print the SA.
        if len(sa.get("acbr", "")) < 1:
            continue

        # Add the total online to the running total for that VO; if necessary,
        # calculate the amount of space to reduce this SA by.
        for vo in get_vos_from_acbr(sa.get("acbr", "")):
            if vo in vo_limit:
                try:
                    cumulative_total[vo] += totalOnline
                except:
                    pass
                reduce_by_amount = max(reduce_by_amount,
                    cumulative_total.get(vo, 0) - vo_limit[vo])
        # If we reduce more than the total amount, skip
        if reduce_by_amount > totalOnline:
            continue
        elif reduce_by_amount > 0:
            # Recalculate the total, available, and free space to be consistent.
            sa['totalOnline'] = str(int(totalOnline - reduce_by_amount))
            try:
                sa['availableSpace'] = str(int(max(0,
                    float(sa['availableSpace']) - reduce_by_amount*1024**2)))
                sa['freeOnline'] = str(int(max(0, float(sa['freeOnline']) \
                    - reduce_by_amount)))
            except:
                pass
        # Calculated the allocated amount.  If there is only one VO which can
        # access this SA, then the allocated amount = total space.
        # Otherwise, allocated is zero.
        # If the provider already set allocatedOnline, we ignore this logic.
        if sa.get('allocatedOnline', None) == None:
            if len(get_vos_from_acbr(sa.get('acbr', ''))) > 1:
                sa['allocatedOnline'] = 0
            else:
                sa['allocatedOnline'] = sa.get('totalOnline', 0)
        # Check that installed capacity is >= total online
        if sa.get('installedOnlineCapacity', None) == None:
            sa['installedOnlineCapacity'] = sa.get('totalOnline', 0)
        if sa.get('installedNearlineCapacity', None) == None:
            sa['installedNearlineCapacity'] = sa.get('totalNearline', 0)
        # Finally, print out the SA.
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
    info.setdefault('installedOnlineCapacity', info.get('totalOnline', 0))
    info.setdefault('installedNearlineCapacity', info.get('totalNearline', 0))
    info.setdefault('totalOnline', 0)
    info.setdefault('usedOnline', 0)
    info.setdefault('freeOnline', 0)
    info.setdefault('reservedOnline', 0)
    info.setdefault('allocatedOnline', 0)
    info.setdefault('totalNearline', 0)
    info.setdefault('usedNearline', 0)
    info.setdefault('freeNearline', 0)
    info.setdefault('reservedNearline', 0)
    info.setdefault('retention', 'replica')
    info.setdefault('accessLatency', 'online')
    info.setdefault('expiration', 'neverExpire')
    info.setdefault('availableSpace', 0)
    info.setdefault('usedSpace', 0)

    if has_gratia_capacity:
        try:
            default_path = os.path.expandvars("$VDT_LOCATION/gratia/probe" \
                "/service/ProbeConfig")
            Gratia.Initialize(cp_get("gip", "ProbeConfig", default_path))
            desc = StorageElement.StorageElement()
            uniqueID = '%s:%s:%s' % (se_unique_id, "GlueStorageArea",
                info['saName'])
            parentID = '%s:%s:%s' % (se_unique_id, "SE", se_unique_id)
            state.ParentID(info.get("seUniqueID", "UNKNOWN"))
            desc.UniqueID(uniqueID)
            desc.Name(info['saName'])
            desc.SpaceType('GlueStorageArea')
            desc.Implementation(se.getImplementation())
            desc.Version(se.getVersion())
            desc.Status(se.getStatus())
            Gratia.Send(desc)

            if int(info['totalNearline']) > 0:
                state = StorageElementRecord.StorageElementRecord()
                state.UniqueID(uniqueID)
                state.MeasurementType("raw")
                state.StorageType("tape")
                state.TotalSpace(str(info['totalNearline']))
                state.FreeSpace(str(info['freeNearline']))
                state.UsedSpace(str(info['usedNearline']))
                Gratia.Send(state)
            if int(info['totalOnline']) > 0:
                state = StorageElementRecord.StorageElementRecord()
                state.UniqueID(uniqueID)
                state.MeasurementType("raw")
                state.StorageType("disk")
                state.TotalSpace(str(info['totalOnline']))
                state.FreeSpace(str(info['freeOnline']))
                state.UsedSpace(str(info['usedOnline']))
                Gratia.Send(state)
        except Exception, e:
            log.exception(e)
    printTemplate(saTemplate, info)

def get_vos_from_acbr(acbr):
    acbr_lines = acbr.splitlines()
    vos = sets.Set()
    for line in acbr_lines:
        acbr = line.split()[-1]
        try:
            acbr = normalizeFQAN(acbr)
            acbr = acbr.split('/')
            if len(acbr) == 1 or len(acbr[0]) > 0:
                continue
            acbr = acbr[1]
        except:
            continue
        vos.add(acbr)
    return vos

def print_VOInfo(se, cp):
    """
    Print out the VOInfo GLUE information for all the VOInfo
    objects in the SE.

    This will optionally alter the VOInfo object to limit the total available
    space for a given VO.
    """
    vo_limit_str = cp_get(cp, "se", "vo_limits", "")
    vo_limit = {}
    cumulative_total = {}
    for vo_str in vo_limit_str.split(','):
        vo_str = vo_str.strip()
        info = vo_str.split(":")
        if len(info) != 2:
            continue
        vo = info[0].strip()
        try:
            limit = float(info[1].strip())
        except:
            continue
        vo_limit[vo] = limit
        cumulative_total.setdefault(vo, 0)
    for voinfo in se.getVOInfos():
        do_continue = False
        reduce_by_amount = 0
        try:
            totalOnline = float(voinfo.get("totalOnline", 0))
        except:
            continue
        for vo in get_vos_from_acbr(voinfo.get("acbr", "")):
            if vo in vo_limit:
                try:
                    cumulative_total[vo] += totalOnline
                except:
                    pass
                reduce_by_amount = max(reduce_by_amount,
                    cumulative_total.get(vo, 0) - vo_limit[vo])
        if reduce_by_amount > totalOnline:
            continue
        elif reduce_by_amount > 0:
            voinfo['totalOnline'] = str(totalOnline - reduce_by_amount)
            try:
                voinfo['availableSpace'] = str(max(0,
                    float(voinfo['availableSpace']) - reduce_by_amount*1024**2))
                voinfo['freeOnline'] = str(max(0, float(voinfo['freeOnline']) \
                    - reduce_by_amount))
            except:
                pass
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
    if not cp_getBoolean(cp, "classic_se", "advertise_se", True):
        log.info("Not advertising a classic SE.")
        return
    else:
        log.info("Advertising a classic SE.")
    #if cp_getBoolean(cp, "se", "shares_fs_with_ce", False):
    #    log.info("Not advertising a classic SE because the SE shares a FS.")
    #    return

    status = cp_get(cp, "classic_se", "status", "Production")
    version = cp_get(cp, "classic_se", "version", "UNKNOWN")
    try:
        used, available, total = getClassicSESpace(cp, total=True, gb=True)
    except Exception, e:
        log.error("Unable to get SE space: %s" % str(e))
        used, available, total = 0, 0, 0

    # Tape information, if we have it...
    nu, _, nt = getSETape(cp)

    siteUniqueID = cp_get(cp, "site", "unique_name", "UNKNOWN")
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
             "bdii"           : cp_get(cp, "bdii", "endpoint", "Unknown"),
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
            "installedOnlineCapacity": 0,
            "installedNearlineCapacity": 0,
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
            "allocatedOnline"  : 0,
           }
    saTemplate = getTemplate("GlueSE", "GlueSALocalID")
    printTemplate(saTemplate, info)

    print_classic_access(cp, siteUniqueID)

def print_SE(se, cp):
    """
    Emit the GLUE entities for the SE, based upon the StorageElement class.
    """

    # if the unique ID is UNKNOWN, a real SE does not exist, the classic SE
    # will probably be invoked
    seUniqueID = se.getUniqueID()
    if seUniqueID == "UNKNOWN" or seUniqueID == "UNAVAILABLE": return

    status = se.getStatus()
    version = se.getVersion()

    # Determine space information
    try:
        used, available, total = se.getSESpace(total=True, gb=True)
    except:
        used, available, total = 0, 0, 0

    # Tape information, if we have it...
    nu, nf, nt = se.getSETape()

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
             "bdii"           : cp_get(cp, "bdii", "endpoint", "Unknown"),
             "siteUniqueID"   : siteUniqueID,
             "arch"           : arch,
           }
    seTemplate = getTemplate("GlueSE", "GlueSEUniqueID")
    log.info(str(info))
    printTemplate(seTemplate, info)

    if has_gratia_capacity:
        try:
            default_path = os.path.expandvars("$VDT_LOCATION/gratia/probe" \
                "/service/ProbeConfig")
            Gratia.Initialize(cp_get("gip", "ProbeConfig", default_path))
            desc = StorageElement.StorageElement()
            uniqueID = '%s:%s:%s' % (info['seUniqueID'], "SE",
                info['seUniqueID'])
            desc.UniqueID(uniqueID)
            desc.Name(info['seName'])
            desc.SpaceType('SE')
            desc.Implementation(implementation)
            desc.Version(version)
            desc.Status(status)
            Gratia.Send(desc)

            if int(nt) > 0:
                state = StorageElementRecord.StorageElementRecord()
                state.UniqueID(uniqueID)
                state.MeasurementType("raw")
                state.StorageType("tape")
                state.TotalSpace(str(nt))
                state.FreeSpace(str(nf))
                state.UsedSpace(str(nu))
                Gratia.Send(state)
            if int(total) > 0:
                state = StorageElementRecord.StorageElementRecord()
                state.UniqueID(uniqueID)
                state.MeasurementType("raw")
                state.StorageType("disk")
                state.TotalSpace(str(total))
                state.FreeSpace(str(available))
                state.UsedSpace(str(used))
                Gratia.Send(state)
        except Exception, e:
            log.exception(e)


    try:
        print_SA(se, cp, se.getSection())
    except Exception, e:
        log.exception(e)
    try:
        print_VOInfo(se, cp)
    except Exception, e:
        log.exception(e)

    advertise_accesspoints = cp_getBoolean(cp, "gip", "advertise_accesspoints", "True")
    if advertise_accesspoints:
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
        except Exception, e:
            log.exception(e)

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
    info.setdefault('statusInfo', 'OK')
    endpoint = info.get('endpoint', 'httpg://example.org:8443/srm/managerv2')

    # Filter endpoint to make it acceptable!
    endpoint.replace('srm://', 'httpg://')
    sfn_loc = endpoint.find('?SFN=')
    if sfn_loc >= 0:
        endpoint = endpoint[:sfn_loc]
    info['protocolType'] = 'SRM'
    info['serviceType'] = 'SRM'
    info['capability'] = 'control'
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
        info["wsdl"] = "http://sdm.lbl.gov/srm-wg/srm.v1.1.wsdl"
        info["semantics"] = "http://sdm.lbl.gov/srm-wg/srm.v1.1.wsdl"
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
                                   info['hostname'] + "_" + str(info['port'])
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

def determine_provider(provider_implementation, implementation, cp):
    """
    Determine which provider class to use based upon the requested provider
    implementation and the actual SE implementation
    """
    provider_implementation = provider_implementation.strip().lower()
    implementation = implementation.strip().lower()
    if provider_implementation == 'static':
        se_class = StorageElement
    elif provider_implementation == 'bestman':
        se_class = BestmanInfo
    elif provider_implementation == 'dcache':
        cp = config("$GIP_LOCATION/etc/dcache_storage.conf")
        se_class = DCacheInfo
    elif provider_implementation == 'dcache19':
        se_class = DCacheInfo19
    elif implementation.find('bestman') >= 0:
        se_class = BestmanInfo
    elif implementation.find('dcache') >= 0:
        cp = config("$GIP_LOCATION/etc/dcache_storage.conf")
        se_class = DCacheInfo
    else:
        se_class = StorageElement
    return se_class, cp

def handle_SE(cp, section):
    """
    Run a provider for one SE.
    """
    # if you only have a classic SE, there will still be a [se] section
    # with the default_se option.  This will result in a GlueService
    # stanza being written with garbage information in it. So, check
    # for default_se = "UNKNOWN" or "UNAVAILABLE" and return if it does
    #
    # default_se is set in the [Storage] section in config.ini and is
    # required by configure-osg.py
    default_se = cp_get(cp, "se", "default_se", "UNKNOWN")
    if default_se == "UNAVAILABLE" or default_se == "UNKNOWN": return

    impl = cp_get(cp, section, "implementation", "UNKNOWN")
    provider_impl = cp_get(cp, section, "provider_implementation", "UNKNOWN")
    if provider_impl == "UNKNOWN" and not cp_getBoolean(cp, section,
            "dynamic_dcache", False):
        provider_impl = 'static'
    se_class, cp = determine_provider(provider_impl, impl, cp)
    se = se_class(cp, section=section)
    log.info("Outputting SE %s; implementation %s; provider %s" % (se.getName(),
        impl, provider_impl))
    try:
        se.run()
    except Exception, e:
        log.exception(e)

    # Print out the SE-related portions
    try:
        print_SE(se, cp)
    except Exception, e:
        log.exception(e)

    # Print out the SRM-related portions
    try:
        print_SRM(se, cp)
    except Exception, e:
        log.exception(e)

def main():
    """
    The primary wrapper function for emitting GLUE for storage elements.
    """
    cp = config()

    # Handle full-fledged SEs
    found_se = False
    for section in cp.sections():
        # need to search for sections with "_" because if you are only
        # advertising a classic SE, then you do NOT want to run the
        # handle_SE function or you will get duplicate and incorrect info
        if section.lower().startswith("se_"):
            advertise_se = cp_getBoolean(cp, section, "advertise_se", True)
            if advertise_se:
                handle_SE(cp, section)
            found_se = True
    if found_se == False and 'se' in cp.sections():
        handle_SE(cp, 'se')

    # Handle the "classic" SE.
    try:
        print_classicSE(cp)
    except Exception, e:
        log.exception(e)

if __name__ == '__main__':
    main()

