
import re
import gip_sets as sets
import urllib2
import socket

from gip_common import getLogger, cp_get, normalizeFQAN
from gip_storage import StorageElement, voListStorage
from DCacheInfoProviderParser import parse_fp
from space_calculator import getAllowedVOs, getLGAllowedVOs

log = getLogger("GIP.Storage.dCache")

class DCacheInfo19(StorageElement):

    def __init__(self, cp, **kw):
        super(DCacheInfo19, self).__init__(cp, **kw)
        self.status = 'Production'
        self.dom = None
        self.sas = []
        self.vos = []
        self.seen_pools = sets.Set()

    def run(self):
        endpoint = cp_get(self._cp, self._section, "infoProviderEndpoint", "")
        try:
            self.handler = parse_fp(urllib2.urlopen(endpoint))
        except Exception, e:
            log.exception(e)
            self.handler = None
        self.parse()

    def parse(self):
        self.parseSAs_fromLG()
        self.parseSAs_fromPG()
        self.parseVOInfos_fromReservations()

    def getPort(self):
        port = cp_get(self._cp, self._section, "srm_port", "8443")
        return port

    def getSESpace(self, gb=False, total=False):
        total = self.handler.summary.get('total', 0) / 1000
        free = self.handler.summary.get('free', 0) / 1000
        used = self.handler.summary.get('used', 0) / 1000
        if gb:
            total /= 1000**2
            free /= 1000**2
            used /= 1000**2
        if total:
            return used, free, total
        return used, free, total

    def parseVOInfos_fromReservations(self):
        log.debug("Starting 'parseVOInfos_fromReservations'")
        seUniqueID = self.getUniqueID()

        # The VOInfo objects should correspond to reservation DESCRIPTIONs,
        # which may map to multiple reservations
        voinfo = {}
        log.debug("There are %i reservations." % len(self.handler.reservations))
        for id, reservation in self.handler.reservations.items():
            desc = reservation.get('description', 'DEFAULT')
            if desc.lower() == 'null':
                desc = 'DEFAULT'
            reservation_set = voinfo.setdefault(desc, [])
            reservation_set.append(reservation)

        log.debug("VOInfo objects: %s" % str(voinfo))

        # Iterate through all the link groups, and then go through all the
        # reservations in that link group.
        for lgid, linkgroup in self.handler.linkgroups.items():
            # Determine ACBRs and allowed VOs
            found_reservation_vos = sets.Set()
            allowed_acbrs = getLGAllowedVOs(self._cp,
                linkgroup.get('acbrs', ''), linkgroup.get('name', ''))
            fqans = [normalizeFQAN(i) for i in allowed_acbrs]
            all_lg_vos = sets.Set([i.split('/')[1] for i in fqans])

            # Find the corresponding SA
            found_sa = False
            lgname = linkgroup.get('name', '')
            for sa in self.sas:
                if sa.get('name', None) != lgname:
                    continue
                found_sa = True
                break
            if not found_sa:
                continue

            # Advertise the right tag for any VO which has a space reservation
            # in this link group.
            for tag, reservations_set in voinfo.items():
                for info in reservations_set:
                    if info.get('linkgroupref', None) != lgid:
                        continue
                    log.debug("Analyzing reservation: %s" % str(info))
                    # Regardless of who owns the reservation, anyone who can
                    # write into the LG can write into the reservation.
                    #if len(str(info.get('FQAN', ''))) == 0:
                    #    allowed_acbrs = getLGAllowedVOs(self._cp, info.get(
                    #        'acbrs', ''))
                    #else:
                    #    allowed_acbrs = ['VOMS:' +str(info.get('FQAN', ''))]
                    fqans = [normalizeFQAN(i) for i in allowed_acbrs]
                    vos = [i.split('/')[1] for i in fqans]
                    found_reservation_vos.update(vos)
                    if len(vos) == 0:
                        continue
                    path = self.getPathForSA(lgname, vos[0],
                        section=self._section)
                    myid = '%s:%s' % (tag, info.get('id', 'UNKNOWN'))
                    info = {'voInfoID': myid,
                            'seUniqueID': seUniqueID,
                            'name': myid,
                            'path': path,
                            'tag': tag,
                            'acbr': '\n'.join(['GlueVOInfoAccessControlBaseRule: '\
                                '%s' % i for i in allowed_acbrs]),
                            'saLocalID': sa['saLocalID'],
                            }
                    self.vos.append(info)


            # For any VO with no pre-existing reservation, but which is allowed
            # to make reservations, also do a VOInfo object.
            for vo in all_lg_vos.difference(found_reservation_vos):
                vo = normalizeFQAN(i).split('/')[1]
                path = self.getPathForSA(lgname, vo,
                    section=self._section)
                id = '%s' % vo
                info = {'voInfoID': id,
                        'seUniqueID': seUniqueID,
                        'name': id,
                        'path': path,
                        'tag': '%s with no reserved space' % vo,
                        'acbr': 'GlueVOInfoAccessControlBaseRule: VO:%s' % vo,
                        'saLocalID': sa['saLocalID'],
                       }
                self.vos.append(info)

    def parseSAs_fromLG(self):
        default_dict = { \
            "seUniqueID": self.getUniqueID(),
            "accessLatency": "online",
            "retention": "replica",
        }
        def itemgetter(x, y):
            return cmp(x.get('total', 0), y.get('total', ))
        log.info(self.handler.links)
        sorted_poolgroups = self.handler.poolgroups.values()
        sorted_poolgroups.sort(itemgetter)
        has_seen_matching_poolgroup = False

        has_links = False
        for linkgroup in self.handler.linkgroups.values():
            has_links = True
            info = dict(default_dict)
            space = linkgroup.get('name', None)
            if not space:
                continue
            info['name'] = space
            info['path'] = self.getPathForSA(space, vo='', return_default=True,
                section=self._section)
            if linkgroup.get('nearlineAllowed', False):
                info['accessLatency'] = 'nearline'
            info['totalOnline'] = linkgroup.get('total', 0) / 1000**3
            info['reservedOnline'] = linkgroup.get('reserved', 0) / 1000**3
            info['usedOnline'] = linkgroup.get('used', 0) / 1000**3
            info['freeOnline'] = linkgroup.get('free', 0) / 1000**3
            info['usedSpace'] = linkgroup.get('used', 0) / 1024
            info['availableSpace'] = linkgroup.get('free', 0) / 1024
            info['saName'] = '%s:%s:%s' % (info['name'], info['retention'],
                info['accessLatency'])
            info['saLocalID'] = info['saName']
            acbr_attr = 'GlueSAAccessControlBaseRule: %s'
            log.info(linkgroup['acbrs'])
            acbr = '\n'.join([acbr_attr % i for i in getLGAllowedVOs(self._cp,
                linkgroup.get('acbrs', ''), info['name'])])
            info['acbr'] = acbr
            if len(acbr) == 0:
                continue

            # Look at all the pool groups and see if the size of any one of them
            # is approximately the size of our link group.  If so, assume we've
            # seen the pools in the pool group.
            # Do not do this if the total size is zero.
            if not info['totalOnline']:
                continue
            for pool_group in sorted_poolgroups:
                if abs(pool_group.get('total', 0) - linkgroup['total']) / \
                        float(linkgroup['total']) <= 0.01:
                    self.seen_pools.update(pool_group.get('pools', sets.Set()))
                    has_seen_matching_poolgroup = True
                    break
            self.sas.append(info)
        # Unfortunately, the info provider has no mechanism for matching link
        # groups to pool groups.  Hence, if we haven't matched the link group
        # then we assume that we've seen the whole SE.
        if has_links and not has_seen_matching_poolgroup:
            self.seen_pools.update(self.handler.pools.keys())

    def parseSAs_fromPG(self):
        seen_pools = self.seen_pools
        def itemgetter(x, y):
            return cmp(x.get('total', 0), y.get('total', ))
        log.info(self.handler.links)
        sorted_poolgroups = self.handler.poolgroups.values()
        sorted_poolgroups.sort(itemgetter)
        pools = self.handler.pools
        seUniqueID = self.getUniqueID()
        default_dict = { \
            "seUniqueID": seUniqueID,
            "accessLatency": "online",
            "retention": "replica",
        }
        for poolgroup in sorted_poolgroups:
            info = dict(default_dict)
            if 'name' not in poolgroup:
                continue
            info['name'] = poolgroup['name']
            if poolgroup.get('total', 0) == 0:
                continue
            total = poolgroup.get('total', 0)
            used = poolgroup.get('used', 0)
            free = poolgroup.get('free', 0)
            for pool in poolgroup['pools']:
                if pool in seen_pools:
                    total -= pools.get(pool, {}).get('total', 0)
                    used -= pools.get(pool, {}).get('used', 0)
                    free -= pools.get(pool, {}).get('free', 0)
                else:
                    seen_pools.add(pool)
            if total <= 0:
                continue
            or_func = lambda x, y: x or y
            log.info(poolgroup['links'])
            can_write = reduce(or_func, [self.handler.links.get(i, {}).get('write', 0) \
                > 0 for i in poolgroup['links']], False)
            can_read = reduce(or_func, [self.handler.links.get(i, {}).get('read', 0) \
                > 0 for i in poolgroup['links']], False)
            can_p2p = reduce(or_func, [self.handler.links.get(i, {}).get('p2p', 0) \
                > 0 for i in poolgroup['links']], False)# and allow_p2p
            can_stage = reduce(or_func, [self.handler.links.get(i, {}).get('cache', 0) \
                > 0 for i in poolgroup['links']], False)# and allow_staging

            accesslatency = 'online'
            retentionpolicy = 'replica'
            if can_stage:
                accesslatency = 'nearline'
                retentionpolicy = 'custodial'
            info['saLocalID'] = '%s:%s:%s' % (info['name'], retentionpolicy,
                accesslatency)
            info['saName'] = info['saLocalID']
            if can_stage:
                expirationtime = 'releaseWhenExpired'
            else:
                expirationtime = 'neverExpire'
            info['accessLatency'] = accesslatency
            info['retention'] = retentionpolicy

            info['totalOnline'] = max(total, 0) / 1000**3
            info['usedOnline'] = max(used, 0) / 1000**3
            info['freeOnline'] = max(free, 0) / 1000**3
            info['usedSpace'] = max(used, 0) / 1024
            info['availableSpace'] = max(free, 0) / 1024
            info['path'] = self.getPathForSA(info['name'],
                section=self._section, return_default=True)
            info['acbrs'] = getAllowedVOs(self._cp, info['name'])
            acbr_attr = 'GlueSAAccessControlBaseRule: %s'
            acbr = [acbr_attr % i for i in info['acbrs']]
            acbr += [acbr_attr % i.split(':')[-1] for i in info['acbrs']]
            acbr = '\n'.join(acbr)
            info['acbr'] = acbr
            self.sas.append(info)

            # Create VOInfo objects.
            for vo in getAllowedVOs(self._cp, poolgroup['name']):
                if vo.startswith('VO:'):
                    vo = vo[3:]
                path = self.getPathForSA(poolgroup['name'], vo=vo,
                    section=self._section)
                id = '%s:%s:poolgroup' % (vo, poolgroup['name'])
                acbr = 'GlueVOInfoAccessControlBaseRule: %s' % vo
                voinfo = {'voInfoID': id,
                    'seUniqueID': seUniqueID,
                    'name': id,
                    'path': path,
                    'tag': 'VOInfo associated with poolgroup %s; no space' \
                        ' reservation' % poolgroup['name'],
                    'acbr': acbr,
                    'saLocalID': info['saName']
                }
                self.vos.append(voinfo)


    def getSAs(self):
        if getattr(self, 'sas', None):
            return self.sas
        return super(DCacheInfo19, self).getSAs()

    def getVOInfos(self):
        if getattr(self, 'vos', None) != None:
            return self.vos
        return super(DCacheInfo19, self).getVOInfos()

    def getVersion(self):
        try:
            return self.handler.version
        except Exception, e:
            log.exception(e)
            return super(DCacheInfo19, self).getVersion()

    def getAccessProtocols(self):
        aps = []
        for door in self.handler.doors.values():
            if door.get('family', '') == 'SRM':
                continue
            if not door.get('name', ''):
                continue
            port = door.get('port', 0)
            version = door.get('version', '0.0.0')
            protocol = door.get('family', 'UNKNOWN')
            hostname = door.get('FQDN', '')
            try:
                hostname = socket.getfqdn(hostname)
            except:
                pass
            endpoint = "%s://%s:%i" % (protocol, hostname, int(port))
            securityinfo = ''
            if protocol.startswith('gsiftp'):
                securityinfo = "GSI"
            elif protocol == 'dcap':
                securityinfo = "None"
            else:
                securityinfo = "None"
            info = {'accessProtocolID': door.get('name', ''),
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
        prev_srm_host = None
        try:
            info = super(DCacheInfo19, self).getSRMs()
            endpoint = info[0]['endpoint']
            srmhost_re = re.compile("://(.*?):")
            m = srmhost_re.search(endpoint)
            if m:
                prev_srm_host = m.groups()[0]
        except:
            raise
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

            if prev_srm_host:
                srm_host = prev_srm_host

            # Determine the VOs which are allowed to use this storage element
            acbr_tmpl = '\nGlueServiceAccessControlRule: VO:%s' \
                '\nGlueServiceAccessControlRule: %s'
            acbr = ''
            
            vos = voListStorage(self._cp, section=self._section)
            for vo in vos:
                acbr += acbr_tmpl % (vo, vo)
            acbr = acbr[1:]
            srms = []

            # Use the srm-LoginBroker cell to list all the SRM cells available.
            for srm in self.handler.doors.values():
                if srm.get('family', '') != 'SRM':
                    continue
                if not srm.get('name', ''):
                    continue
                doorname = srm.get('name', '')
                hostname = srm.get('FQDN', '')
                port = srm.get('port', 0)
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

                if prev_srm_host:
                    hostname = prev_srm_host

                # From the SRM info, build the information for the GLUE entity.
                info = {
                    "serviceType"  : "SRM",
                    "acbr"         : acbr,
                    "cpLocalID"    : srm.get('name', ''),
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
            return super(DCacheInfo19, self).getSRMs()

    def hasSRM(self):
        """
        Return True if there is a SRM endpoint present on this SE.
        Dcache is an SRM server, so return True
        """
        return True
