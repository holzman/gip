
import re
import gip_sets as sets

from gip_common import cp_get, getLogger
from gip_storage import StorageElement, voListStorage
import srm_ping

log = getLogger('GIP.Storage.Bestman')

class BestmanInfo(StorageElement):

    def __init__(self, cp, **kw):
        super(BestmanInfo, self).__init__(cp, **kw)
        srms = self.getSRMs()
        if not srms:
            raise ValueError("No SRM endpoint configured!")
        self.srm_info = srms[0]
        self.endpoint = self.srm_info['endpoint']
        self.info = {}
        self.status = False

    def run(self):
        try:
            self.info = srm_ping.bestman_srm_ping(self._cp, self.endpoint,
                section=self._section)
            log.info("Returned BestMan info: %s" % str(self.info))
            self.status = True
        except Exception, e:
            log.exception(e)
            self.status = False
        try:
            self.parse_SAs()
        except Exception, e:
            log.exception(e)
            self.sas = []
            self.voinfos = []
  
    def getSRMs(self):
        srms = super(BestmanInfo, self).getSRMs()
        for srm in srms:
            if srm['endpoint'].find('/srm/managerv2') >= 0:
                srm['endpoint'] = srm['endpoint'].replace( \
                    '/srm/managerv2', '/srm/v2/server')
        return srms
 
    gftp_url_re = re.compile('(.+)://(.+)')
    def getAccessProtocols(self):
        results = []
        gftps = self.info.get('gsiftpTxfServers', '')
        for gftp in gftps.split(';'):
            m = self.gftp_url_re.match(gftp)
            if m:
                protocol, host = m.groups()
                host_info = host.split(':')
                if len(host_info) == 2:
                    host, port = host_info
                else:
                    port = '2811'
                ap_info = {'protocol': protocol, 'hostname': host,
                    'port': port, 'version': '1.0.0'}
                results.append(ap_info)
        return results

    def getVersion(self):
        try:
            version = self.info.get('backend_version', None)
        except:
            version = None
        if not version:
            return super(BestmanInfo, self).getVersion()
        return version

    def parse_SAs(self):
        self.sas = []
        self.voinfos = []
        cntr = 0
        token = self.info.get('staticToken(%i)' % cntr, None)
        while token:
            sa_info = {}
            sa_name = None
            for info in token.split():
                try:
                    key, val = info.split('=')
                except:
                    val = info
                    key = 'name'
                if key == 'name':
                    sa_info['saLocalID'] = '%s:%s:%s' % (val, 'replica',
                        'online')
                    sa_name = val
                elif key == 'size':
                    try:
                        size = int(val)
                        size_kb = size/1024
                        size_gb = size_kb/1024**2
                    except Exception, e:
                        log.exception(e)
                        size, size_kb, size_gb = 0, 0, 0
                    sa_info['totalOnline'] = size_gb
                    sa_info['reservedOnline'] = size_gb
            sa_info['path'] = self.getPathForSA(space=sa_name)
            vo_info = {}
            vos = self.getVOsForSpace(sa_name)
            sa_vos = sets.Set()
            for vo in vos:
                sa_vos.add(vo)
                if not vo.startswith('VO'):
                    sa_vos.add('VO: %s' % vo)
            sa_vos = list(sa_vos)
            sa_vos.sort()
            sa_info['acbr'] = '\n'.join(['GlueSAAccessControlBaseRule: %s' % i \
                for i in sa_vos])
            for vo in self.getVOsForSpace(sa_name):
                id = '%s:%s' % (vo, sa_name)
                tag = sa_info.get('tag', sa_name)
                path = self.getPathForSA(space=sa_name, vo=vo)
                info = {'voInfoID': id,
                        'name': 'BeStMan static space %s for VO %s' % (sa_name,
                            vo),
                        'path': path,
                        'tag': tag,
                        'acbr': 'GlueVOInfoAccessControlBaseRule: %s' % vo, 
                        'saLocalID': sa_info.get('saLocalID', 'UNKNOWN_SA')
                       }
                self.voinfos.append(info)
            self.sas.append(sa_info)
            cntr += 1
            token = self.info.get('staticToken(%i)' % cntr, None)

    def getVOsForSpace(self, space):
        all_vos = voListStorage(self._cp)
        if space:
            myspace = space.lower()
        else:
            myspace = ''
        for vo in all_vos:
            myvo = vo.lower()
            if myspace.find(myvo) >= 0:
                return [myvo]
        return super(BestmanInfo, self).getVOsForSpace(space)

    def getSAs(self):
        if not self.sas:
            return super(BestmanInfo, self).getSAs()
        return self.sas

    def getVOInfos(self):
        if not self.voinfos:
            return super(BestmanInfo, self).getVOInfos()
        return self.voinfos

    def getSESpace(self, gb=False, total=False):
        if total:
            used, free, tot = super(BestmanInfo, self).getSESpace(gb=gb,
                total=total)
            try:
                tot = 0
                for sa in self.getSAs():
                    tot += int(sa.setdefault('totalOnline', 0))
                if not gb:
                    tot *= 1000**2
            except Exception, e:
                log.exception(e)
            return used, free, tot
        else:
            return super(BestmanInfo, self).getSESpace(gb=gb, total=total)
          
