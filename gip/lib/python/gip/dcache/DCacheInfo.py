
import socket

from gip_common import cp_get
from gip_logging import getLogger
from gip_storage import StorageElement, voListStorage, getdCacheSESpace
from admin import connect_admin
from space_calculator import calculate_spaces

log = getLogger("GIP.Storage.dCache")

class DCacheInfo(StorageElement):

    def __init__(self, cp, **kw):
        super(DCacheInfo, self).__init__(cp, **kw)
        self.status = 'Production'

    def run(self):
        try:
            self.admin = connect_admin(self._cp)
        except Exception, e:
            log.exception(e)
            self.status = 'Closed'
        try:
            self.sas, self.vos = calculate_spaces(self._cp, self.admin,
                section=self._section)
        except Exception, e:
            log.exception(e)
    
    def getSAs(self):
        if getattr(self, 'sas', None):
            return self.sas
        return super(DCacheInfo, self).getSAs()

    def getSESpace(self, gb=False, total=False):
        return getdCacheSESpace(self._cp, self.admin, gb=gb, total=total)

    def getVOInfos(self):
        if getattr(self, 'vos', None):
            return self.vos
        return super(DCacheInfo, self).getVOInfos()

    def getSEVersion(self):
        try:
            return getSEVersion(self._cp, self.admin)
        except Exception, e:
            log.exception(e)
            return super(DCacheInfo, self).getSEVersion()

    def getAccessProtocols(self):
        aps = []
        results = self.admin.execute("LoginBroker", "ls")
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
                    'protocol'        : protocol,
                    'endpoint'        : endpoint,
                    'capability'      : 'file transfer',
                    'maxStreams'      : 10,
                    'security'        : securityinfo,
                    'port'            : port,
                    'version'         : version,
                    'hostname'        : hostname,
                   }
            aps.append(info)
        return aps

    def getSRMs(self):
        try:
            # BUGFIX: Resolve the IP address of srm host that the admin
            # specifies.  If this IP address matches the IP address given by
            # dCache, then we will print out the admin-specified hostname 
            # instead of looking it up.  This is for sites where the SRM host 
            # is a CNAME instead of the A name.
            srm_host = cp_get(self._cp, self._section, "srm_host", None)
            srm_ip = None
            if srm_host:
                try:
                    srm_ip = socket.gethostbyname(srm_host)
                except:
                    pass
            #vos = [i.strip() for i in cp.get("vo", "vos").split(',')]

            # Determine the VOs which are allowed to use this storage element
            acbr_tmpl = '\nGlueServiceAccessControlRule: VO:%s' \
                '\nGlueServiceAccessControlRule: %s'
            acbr = ''
            vos = voListStorage(self._cp)
            for vo in vos:
                acbr += acbr_tmpl % (vo, vo)
            acbr = acbr[1:]

            # Use the srm-LoginBroker cell to list all the SRM cells available.
            results = self.admin.execute("srm-LoginBroker", "ls")
            srms = []
            for line in results.split("\n"):
                if len(line.strip()) == 0:
                    continue
                #Lines have the following format:
               #SRM-srm@srm-srmDomain;Storage;{SRM,1.1.1};[srm:8443];<0,300000>;
                doorname, kind, versions, host, logins, dummy = line.split(';')
                protocol, version = versions[1:-1].split(',')
                hostname, port = host[1:-1].split(':')
                hostname = hostname.split(',')[0]
                # Make sure we have a FQDN (dCache has a nasty habit of
                # dropping the domain name internally.
                try:
                    hostname = socket.getfqdn(hostname)
                    hostname_ip = socket.gethostbyname(hostname)
                except:
                    hostname_ip = None 
                if hostname_ip != None and hostname_ip == srm_ip and \
                        srm_host != None:
                    hostname = srm_host
        
                # From the SRM info, build the information for the GLUE entity.
                info = {
                    "serviceType"  : "SRM",
                    "acbr"         : acbr,
                    "cpLocalID"    : doorname,
                    "protocolType" : "SRM",
                    "capability"   : "control",
                    "status"       : "OK",
                    "statusInfo"   : "SRM instance is responding.",
                    "wsdl"         : "http://sdm.lbl.gov/srm-wg/srm.v1.1.wsdl",
                    "semantics"  : "http://sdm.lbl.gov/srm-wg/doc/srm.v1.0.pdf",
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
                # Bugfix: Make the control protocol unique ID unique between 
                # the SRM versions.
                info['cpLocalID'] = doorname + '_srmv1'
                srms.append(info)
                info = dict(info)

                # Change the v1 information to v2 and add it again to the list.
                info['version'] = "2.2.0"
                endpoint = "httpg://%s:%i/srm/managerv2" % (hostname, int(port))
                info['endpoint'] = endpoint
                info['serviceID'] = endpoint
                info['uri'] = endpoint
                info['url'] = endpoint
                info['serviceName'] = endpoint
                info["wsdl"] = "http://sdm.lbl.gov/srm-wg/srm.v2.2.wsdl"
                info["semantics"] = "http://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.pdf"
                # Bugfix: Make the control protocol unique ID unique between 
                # the SRM versions
                info['cpLocalID'] = doorname + '_srmv2'
                srms.append(info)
            return srms

        except Exception, e:
            log.exception(e)
            return super(DCacheInfo, self).getSRMs()

