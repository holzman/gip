
"""
Ping the BeStMan SRM server for information.
"""

import os
import re
import tempfile

import gip_testing
from gip_common import cp_get, getLogger
from gip_testing import runCommand

log = getLogger('GIP.Storage.Bestman.srm_ping')

def which(executable):
    """
    Helper function to determine the location of an executable.

    @param executable: Name of the program.
    @returns: Full path to executable, or None if it can't be found.
    """
    for dirname in os.environ.get('PATH', '/bin:/usr/bin'):
        fullname = os.path.join(dirname, executable)
        if os.path.exists(fullname):
            return fullname
    return None

class ProxyCreateException(Exception):
    pass

def create_proxy(cp, proxy_filename, section='bestman'):
    """
    Attempt to create a very shortlived proxy at a given location.
    """
    #if not which('grid-proxy-init'):
    #    raise ValueError("Could not find grid-proxy-init; perhaps you forgot"\
    #        " to source $VDT_LOCATION/setup.sh in the environment beforehand?")
    usercert = cp_get(cp, section, "usercert", "/etc/grid-security/http/" \
        "httpcert.pem")
    userkey = cp_get(cp, section, "userkey", "/etc/grid-security/http/" \
        "httpkey.pem")
    if not os.path.exists(usercert):
        raise ProxyCreateException("Certificate to create proxy, %s, does not" \
            " exist." % usercert)
    cmd = 'grid-proxy-init -valid 00:05 -cert %s -key %s -out %s' % \
        (usercert, userkey, proxy_filename)
    fd = runCommand(cmd)
    fd.read()
    if fd.close():
        raise ProxyCreateException("Unable to create a valid proxy; failed " \
            "command run by user daemon: %s" % cmd )

def validate_proxy(cp, proxy_filename):
    """
    Determine that there is a valid proxy at a given location

    @param proxy_filename: The file to check
    @returns: True if the proxy is valid in proxy_filename; False otherwise.
    """
    #if not which('grid-proxy-info'):
    #    raise ValueError("Could not find grid-proxy-info; perhaps you forgot"\
    #        " to source $VDT_LOCATION/setup.sh?")
    cmd = 'grid-proxy-info -f %s' % proxy_filename
    fd = runCommand(cmd)
    fd.read()
    if fd.close():
        raise ProxyCreateException("Unable to validate proxy; " \
              "command run by user daemon: %s" % cmd )
    return True

key_re = re.compile('\s*Key=(.+)')
value_re = re.compile('\s*Value=(.+)')
def parse_srm_ping(output):
    """
    Return a dictionary of key-value pairs returned by the SRM backend.

    Old format.  Example:

[brian@red ~]$ srm-ping srm://srm.unl.edu:8446/srm/v2/server
###########################################
SRM_HOME is /mnt/nfs04/opt-2/osg-wn-source/srm-client-lbnl
JAVA_HOME is /mnt/nfs04/opt-2/osg-wn-source/jdk1.5
X509_CERT_DIR = /mnt/nfs04/opt-2/osg-wn-source/globus/TRUSTED_CA
GSI_DAEMON_TRUSTED_CA_DIR = /mnt/nfs04/opt-2/osg-wn-source/globus/TRUSTED_CA
###########################################

SRM-CLIENT: got remote srm object

SRM-PING: Wed Apr 29 11:05:33 CDT 2009 Calling SrmPing Request...
Ping versionInfo=v2.2

Extra information
	Key=backend_type
	Value=BeStMan
	Key=backend_version
	Value=2.2.1.2.e7
	Key=backend_build_date
	Value=2008-12-16T02:00:24.000Z 
	Key=GatewayMode
	Value=Enabled
	Key=gsiftpTxfServers
	Value=gsiftp://dcache06.unl.edu:5000;gsiftp://dcache08.unl.edu:5000;gsiftp://dcache-s01.unl.edu:5000;gsiftp://dcache-s05.unl.edu:5000;gsiftp://dcache-s10.unl.edu:5000;gsiftp://dcache05.unl.edu:5000;gsiftp://red-gridftp1.unl.edu:5000;gsiftp://red-gridftp2.unl.edu:5000;gsiftp://red-gridftp3.unl.edu:5000
	Key=clientDN
	Value=/DC=org/DC=doegrids/OU=People/CN=Brian Bockelman 504307
	Key=gumsIDMapped
	Value=uscms01
	Key=staticToken(0)
	Value=DEFAULT desc=DEFAULT size=10737418240000
    """
    if output.find("Key=Value") >= 0:
        return parse_srm_ping_new(output)
    results = {}
    cur_key = None
    for line in output.splitlines():
        if not cur_key:
            m = key_re.match(line)
            if m:
                cur_key = m.groups()[0]
        else:
            m = value_re.match(line)
            if m:
                val = m.groups()[0]
                results[cur_key] = val
                cur_key = None
    return results

def parse_srm_ping_new(output):
    """
    Parse newer srm-ping output format.  Example:

[daemon@cithep252 1.0.1]$ srm-ping srm://cit-se2.ultralight.org:8443/srm/v2/server -proxyfile /tmp/http_proxy
srm-ping   2.2.1.2.i4  Tue Apr  7 10:07:10 PDT 2009
SRM-Clients and BeStMan Copyright(c) 2007-2009,
Lawrence Berkeley National Laboratory. All rights reserved.
Support at SRM@LBL.GOV and documents at http://datagrid.lbl.gov/bestman
 
Your proxy has only 235
 second left. Please renew your proxy.
Your proxy has only 235 second left.
Please renew your proxy.
SRM-CLIENT: Connecting to serviceurl httpg://cit-se2.ultralight.org:8443/srm/v2/server

SRM-PING: Wed Apr 29 08:58:05 PDT 2009  Calling SrmPing Request...
versionInfo=v2.2

Extra information (Key=Value)
backend_type=BeStMan
backend_version=2.2.1.2.i4
backend_build_date=2009-04-06T17:07:38.000Z 
gsiftpTxfServers[0]=gsiftp://cithep160.ultralight.org:5000
gsiftpTxfServers[1]=gsiftp://cithep251.ultralight.org:5000
GatewayMode=Enabled
clientDN=/DC=org/DC=doegrids/OU=Services/CN=cithep252.ultralight.org
gumsIDMapped=null
    """
    start_extra_info = False
    info = {}
    for line in output.splitlines():
        if not start_extra_info:
            if line.find("Key=Value") >= 0:
                start_extra_info = True
            continue
        try:
            key, val = line.split('=', 2)
        except:
            continue
        info[key] = val
    return info
        

def bestman_srm_ping(cp, endpoint, section='bestman'):
    """
    Perform a srm-ping operation against a BeStMan endpoint and return the
    resulting key-value pairs.

    @param cp: Site's Config object
    @param endpoint: Endpoint to query (full service URL).
    """
    endpoint = endpoint.replace('httpg', 'srm')

    # Hardcode the proxy filename in order to play nicely with our testing fmwk.
    if gip_testing.replace_command:
        proxy_filename = '/tmp/http_proxy'
    else:
        fd, proxy_filename = tempfile.mkstemp()
    results = {}
    try:
        if not gip_testing.replace_command:
            create_proxy(cp, proxy_filename, section=section)
            validate_proxy(cp, proxy_filename)
        cmd = 'srm-ping %s -proxyfile %s' % (endpoint, proxy_filename)
        fp = runCommand(cmd)
        output = fp.read()
        if fp.close():
            log.debug("srm-ping failed; command %s failed with output: %s" % (cmd, output))
            raise ValueError("srm-ping failed.")
        results = parse_srm_ping(output)
    finally:
        try:
            os.unlink(proxy_filename)
        except:
            pass

    ctr = 0
    key = 'gsiftpTxfServers[%i]' % ctr
    while key in results:
        if 'gsiftpTxfServers' not in results:
            results['gsiftpTxfServers'] = results[key]
        else:
            results['gsiftpTxfServers'] = ';'.join([results['gsiftpTxfServers'],
                results[key]])
        del results[key]
        ctr += 1
        key = 'gsiftpTxfServers[%i]' % ctr

    return results

