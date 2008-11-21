
import socket
import urllib2
from xml.dom.minidom import parse

from gip_common import getLogger, cp_get
from gip_storage import StorageElement, voListStorage


log = getLogger("GIP.Storage.dCache")

def to_boolean(bool_str):
    return bool_str == 'true'

def to_integer(int_str):
    try:
        return int(int_str)
    except:
        return 999999

def get_metrics(dom):
    all_metrics = []
    for metric_dom in dom.getElementsByTagName("metric"):
        name = metric_dom.getAttribute("name")
        mtype = metric_dom.getAttribute("type")
        try:
            value = str(linkgroup.firstChild.data).strip()
        except Exception, e:
            continue
        if not name or not mtype:
            continue
        all_metrics.append((name, mtype, value))
    return all_metrics

class DCacheInfo(StorageElement):

    def __init__(self, cp, **kw):
        super(DCacheInfo, self).__init__(cp, **kw)
        self.status = 'Production'
        self.dom = None

    def run(self):
        endpoint = cp_get(self._cp, self._section, "infoProviderEndpoint", "")
        try:
            self.dom = parse(urllib2.urlopen(endpoint))
        except Exception, e:
            log.exception(e)
        self.parseSAs()

    def parseSAs(self):
        self.parsePools
        self.parsePGs
        self.parseSAs_fromLG(self)
        self.parseSAs_fromPG(self)
        self.parseSESpace(self)

    def parseSESpace(self)
        summary_doms = self.dom.getElementsByTagName('summary')
        summary_doms = [i for i in summary_doms if i in self.dom.childNodes]
        se_space = {}
        for summary_dom in summary_doms:
            for name, mtype, value in get_metrics(summary_dom):
                if name == 'total':
                    se_space['total_bytes'] = to_integer(value)
                elif name == 'free':
                    se_space['free_bytes'] = to_integer(value)
                elif name == 'used':
                    se_space['used_bytes'] = to_integer(value)
        self.se_space = se_space

    def getSESpace(self, gb=False, total=False):
        total = self.se_space.get('total_bytes', 0) / 1000
        free = self.se_space.get('free_bytes', 0) / 1000
        used = self.se_space.get('used_bytes', 0) / 1000
        if gb:
            total /= 1000**2
            free /= 1000**2
            used /= 1000**2
        if total:
            return used, free, total
        return used, free, total

    def getLGAllowedVOs(self, lg_info):
        raise NotImplementedError()

    def parseSAs_fromLG(self):
        lgs_dom = [i for i in self.dom.getElementsByTagName("linkgroups") if i\
            in self.dom.childNodes]
        self.linkgroups_dom = []
        self.linkgroups = []
        for lg_dom in lgs_dom:
            self.linkgroups_dom += [i for i in \
                lg_dom.getElementsByTagName("linkgroup") if i in \
                lg_dom.childNodes]
        default_dict = { \
            "seUniqueID": self.getSEUniqueID(),
            "filetype": "permanent",
            "accesslatency": "online",
            "retentionpolicy": "replica",
            "root": "/",
            "totalNearline": 0,
            "usedNearline": 0,
            "freeNearline": 0,
            "reservedNearline": 0,
        }
        for linkgroup_dom in self.linkgroups_dom:
            info = dict(default_dict)
            info['dom'] = linkgroup_dom
            info['path'] = self.getPathForSA(space, vo, return_default=True,
                section=self._section)
            for name, mtype, value in get_metrics(linkgroup_dom):
                if name == 'nearlineAllowed' and to_boolean(value):
                    info['accessLatency'] = 'nearline'
                elif name == 'outputAllowed' and to_boolean(value):
                    info['retention'] = 'output'
                elif name == 'custodialAllowed' and to_boolean(value):
                    info['retention'] = 'custodial'
                elif name == 'name':
                    info['name'] = value
                elif name == 'total':
                    info['totalOnline'] = to_integer(value)
                elif name == 'free':
                    info['freeOnline'] = to_integer(value)
                elif name == 'used':
                    info['usedOnline'] = to_integer(value)
                elif name == 'reserved':
                    info['reservedOnline'] = to_integer(value)
            if 'name' not in info:
                continue
            info['saName'] = '%s:%s:%s' % (info['name'], info['retention'],
                info['accessLatency'])
            acbr_attr = 'GlueSAAccessControlBaseRule: %s'
            acbr = '\n'.join([acbr_attr % i for i in self.getLGAllowedVOs(cp,
                info['name'])])
            self.calculateLGSpaces(info)
            info['acbr'] = acbr
            self.linkgroups.append(info)

    def parseSAs_fromPG(self):

    def parsePools(self):
        pools_dom = self.dom.getElementsByTagName('pools')
        pools_dom = [i for i in pools_dom if i in self.dom.childNodes]
        pools = []
        for pools in pools_dom
            for pool_dom in pools.getElementsByTagName('pool'):
                info = {}
                name = pool_dom.getAttribute('name')
                if not name:
                    continue
                for name, mtype, value in get_metrics(pool_dom):
                    if name == 'total':
                        info['total'] = to_integer(value)
                    elif name == 'free':
                        info['free'] = to_integer(value)
                    elif name == 'used':
                        info['used'] = to_integer(value)
                pools.append(info)
        self.pools = pools
 

    def parsePGs(self):
        pgs = []
        poolgroups_dom = self.dom.getElementsByTagName('poolgroups')
        poolgroups_dom = [i for i in pools_dom if i in self.dom.childNodes]
        for poolgroups in poolgroups_dom:
            for poolgroup_dom in poolgroup.getElementsByTagName( \
                    'poolgroup'):
                info = {'pools': {}}
                for poolref_dom in poolgroup_dom.getElementsByTagName( \
                        'poolref'):
                    pool = poolref_dom.getAttribute('name')
                    if pool and pool in self.pools:
                        info['pools'][pool] = self.pools[pool]
                pgs.append(info)
        self.poolgroups = pgs

    def calculateLGSpaces(info):
        raise NotImplementedError()
    
    def getSAs(self):
        if getattr(self, 'sas', None):
            return self.sas
        return super(DCacheInfo, self).getSAs()

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

