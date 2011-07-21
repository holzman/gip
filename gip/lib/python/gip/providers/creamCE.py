#!/usr/bin/python
    
import re
import sys  
import os
        
if 'GIP_LOCATION' in os.environ:
    sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
    
from gip_common import cp_get, cp_getBoolean, config, getLogger, getTemplate, printTemplate, voList
from gip_common import vdtDir
from gip_testing import runCommand
import gip_sets as sets
import time
import zlib

log = getLogger("GIP.CREAM")

def getUniqueHash(cp):
    # EGI uses unix 'cksum' command; we'll use zlib's crc instead.
    loc = cp_get(cp, 'gip', 'osg_config', vdtDir(os.path.expandvars('$VDT_LOCATION/monitoring/config.ini'),
                                                 '/etc/osg/config.ini'))
    loc = os.path.expandvars(loc)
    try:
        hash = zlib.crc32(loc)
    except:
        log.error('Could not find config.ini for checksum')
        hash = '0008675309'

    return hash

def getCreamVersion(cp):

    """
    Returns the CREAM version
    """
    if 'VDT_LOCATION' in os.environ:
        vdt_version_cmd = os.path.expandvars("$VDT_LOCATION/vdt/bin/") + 'vdt-version --no-wget'
        vdt_version_out = runCommand(vdt_version_cmd).readlines()
    else:
        return 'UNKNOWN'
    
    cream_re = re.compile('gLite CE CREAM\s+(.*?)\s*-.*')
    creamVersion = 'UNKNOWN'
    for line in vdt_version_out:
        m = cream_re.match(line)
        if m:
            creamVersion = m.groups()[0]
            break
    return creamVersion

def buildServiceID(cp):
    ceName = cp_get(cp, "ce", "name", "UNKNOWN_CE")
    gliteSuffix = 'org.glite.ce.CREAM'
    uniqueHash = getUniqueHash(cp)

    serviceID = '%s_%s_%s' % (ceName, gliteSuffix, uniqueHash)
    return serviceID

def getStartTimeAndPid(cp):
    pgrepOut = runCommand('pgrep -f "org.apache.catalina.startup.Bootstrap start"')
    if not pgrepOut: return ''
    
    pid = int(pgrepOut.readlines()[0])
    startTimeOut = runCommand('ps -p %d -o lstart' % pid)
    if not startTimeOut: return ''

    startTime = startTimeOut.readlines()[1].strip()
    log.debug("Tomcat start time is %s" % startTime)
    timeTuple = time.strptime(startTime)

    glueTime = time.strftime('%FT%X', timeTuple)
    timeOffset = time.strftime('%z')

    log.debug("Tomcat time offset is %s" % timeOffset)
    
    if len(timeOffset) > 4:
        timeOffset = '%s:%s' % (timeOffset[0:3], timeOffset[3:])
        glueTime = glueTime + timeOffset

    return (glueTime, pid)    

    
def main():
    log.info('Starting CREAM service provider')
    try:
        cp = config()
        serviceID = buildServiceID(cp)
        siteID = cp_get(cp, "site", "unique_name", 'UNKNOWN_SITE')
        serviceName = '%s-CREAM' % siteID 
        creamVersion = getCreamVersion(cp)
        endpoint = 'https://%s:8443/ce-cream/services' % cp_get(cp, "ce", "name", 'UNKNOWN_CE')
        allVOs = voList(cp)
        acbr = ''
        owner = ''

        log.debug('CREAM VOs are %s' % allVOs)
        if not allVOs:
            log.error("No VOs supported!")
            acbr = '__GIP_DELETEME'
        else:
            acbr = '\n'.join(['GlueServiceAccessControlBaseRule: %s\n' \
                              'GlueServiceAccessControlBaseRule: VO:%s' % (vo, vo) for vo in allVOs])
            owner = '\n' + '\n'.join(['GlueServiceOwner: %s' % vo for vo in allVOs]) # owner needs an extra prepended newline

        pid = -1
        startTime = 'Not Applicable'
        serviceStatus = 'Not OK'
        serviceStatusInfo = 'Could not find tomcat process'

        try:
            (startTime, pid) = getStartTimeAndPid(cp)
            serviceStatus = 'OK'
            serviceStatusInfo = 'Tomcat (%d) is running' % pid
        except:
            log.error('Could not locate tomcat process (pgrep -f "org.apache.catalina.startup.Bootstrap start"'
                      ' probably failed to return any output!)')
        
        info = {'serviceID': serviceID,
                'serviceType': 'org.glite.ce.CREAM',
                'serviceName': serviceName,
                'version': creamVersion,
                'endpoint': endpoint,
                'semantics': 'https://edms.cern.ch/document/595770',
                'owner': owner,
                'url': '__GIP_DELETEME', # deprecated
                'uri': '__GIP_DELETEME', # deprecated
                'status': serviceStatus,
                'statusInfo': serviceStatusInfo,
                'wsdl': 'http://grid.pd.infn.it/cream/wsdl/org.glite.ce-cream_service.wsdl',
                'startTime': startTime,
                'siteID': siteID,
                'acbr': acbr
                }

        template = getTemplate("GlueService", "GlueServiceUniqueID")
        printTemplate(template, info)
    except Exception, e:
        sys.stdout = sys.stderr
        log.error(e)
        raise
