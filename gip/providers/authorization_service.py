#!/usr/bin/python

import sys, time, os
import re
import popen2
from socket import gethostname

# Make sure the gip_common libraries are in our path
sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getTemplate, printTemplate, cp_getBoolean, cp_get
from gip_logging import getLogger
log = getLogger("GIP.authorization_service")
# Retrieve our logger in case of failure

def publish_gridmap_file(cp, template):
    hostname = cp_get(cp, "ce", 'name', gethostname())
    siteID = cp_get(cp, "site", "unique_name", gethostname())

    info = {'serviceID': '%s:gridmap-file' % hostname,
            'serviceType': 'gridmap-file',
            'serviceName': 'Authorization',
            'version': 'UNDEFINED',
            'endpoint': 'Not Applicable',
            'url': 'localhost://etc/grid-security/gridmap-file',
            'uri': 'localhost://etc/grid-security/gridmap-file',
            'status': 'OK',
            'statusInfo': 'Node is configured to use gridmap-file. ' +
               'Did not check if gridmap-file is properly configured.',
            'wsdl': 'Not Applicable',
            'startTime': 'Not Applicable',
            'siteID': siteID,
            'acbr': '__GIP_DELETEME'
            }
    # Spit out our template, fill it with the appropriate info.
    printTemplate(template, info)

def publish_gums(cp, template):
    hostname = cp_get(cp, "ce", 'name', gethostname())
    siteID = cp_get(cp, "site", "unique_name", gethostname())
    gumsConfig = os.getenv('VDT_LOCATION') + \
                 '/gums/config/gums-client.properties'
    gumsConfigFile = open(gumsConfig, 'r')
    gums_re = re.compile('gums.authz\s*=\s*(https://(.*):.*?/(.*))')

    lines = gumsConfigFile.readlines()
    for line in lines:
        m = gums_re.match(line)
        if m: (gums_uri, gums_host) = m.groups()[0:2]

    os.putenv('X509_USER_CERT', '/etc/grid-security/http/httpcert.pem')
    os.putenv('X509_USER_KEY' , '/etc/grid-security/http/httpkey.pem')

    mapping_subject_dn = '/GIP-GUMS-Probe-Identity'
    mapping_subject_name = '`grid-cert-info -subject` '
    gums_command = os.getenv('VDT_LOCATION') + '/gums/scripts/gums-service' + \
                   ' mapUser -s ' + mapping_subject_name + mapping_subject_dn

    (gums_output, pin) = popen2.popen4(gums_command)

    gums_id_re = re.compile('.*\[userName: (.*)\].*')

    status = "Warning"
    statusInfo = "Test mapping failed: if GUMS was not down, check logs" +\
                 " at " + gums_host + ':' + os.getenv('VDT_LOCATION') + \
                 "/tomcat/v55/logs"

    lines = gums_output.readlines()
    for line in lines:
        m = gums_id_re.match(line)
        if m:
            uidMapping = m.groups([0])
            status = "OK"
            statusInfo = "Test mapping successful: user id = %s" % uidMapping
            break
            
    info = {'serviceID': gums_uri,
            'serviceType': 'GUMS',
            'serviceName': 'Authorization',
            'version': 'UNDEFINED',
            'endpoint': gums_uri,
            'url': gums_uri,
            'uri': gums_uri,
            'status': status,
            'statusInfo': statusInfo,
            'wsdl': 'Not Applicable',
            'startTime': 'Not Applicable', 
            'siteID': siteID,
            'acbr': '__GIP_DELETEME'
            }

    printTemplate(template, info)

def main():
    try:
        # Load up the site configuration
        cp = config()

        # Load up the template for GlueService
        # To view its contents, see $VDT_LOCATION/gip/templates/GlueService
        template = getTemplate("GlueService", "GlueServiceUniqueID")
        if not cp_getBoolean(cp, "site", "advertise_gums", True):
            log.info("Not advertising authorization service.")
            return
        else:
            log.info("Advertising authorization service.")

        authfile = open('/etc/grid-security/gsi-authz.conf', 'r')
        authlines = authfile.readlines()
        authmod = re.compile('^(?!#)(.*)libprima_authz_module')
        for line in authlines:
            m = authmod.match(line)
            if m:
                publish_gums(cp, template)
                return
            
        publish_gridmap_file(cp, template)                

    except Exception, e:
        # Log error, then report it via stderr.
        log.exception(e)
        sys.stdout = sys.stderr
        raise

if __name__ == '__main__':
    main()

