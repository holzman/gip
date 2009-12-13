
import sys
import gip_sets as sets

from xml.sax import make_parser, SAXParseException
from xml.sax.handler import ContentHandler, feature_external_ges

from gip_logging import getLogger

log = getLogger("GIP.Storage.dCache.InfoProviderParser")

IN_TOP = 0
IN_POOLS = 1
IN_LINKGROUPS = 2
IN_LINKS = 3
IN_POOLGROUPS = 4
IN_RESERVATIONS = 5
IN_DOORS = 6
IN_SUMMARY = 7
IN_POOLMANAGER = 8
IN_POOLMANAGER_VERSION = 9
IN_SUMMARY_POOLS = 10

class InfoProviderHandler(ContentHandler):

    def __init__(self):
        self.pools = {}
        self.doors = {}
        self.poolgroups = {}
        self.links = {}
        self.linkgroups = {}
        self.reservations = {}
        self.summary = {}
        self.parent = None
        self.state = IN_TOP
        self.curtag = None
        self.startElementCB = {}
        self.endElementCB = {}
        self.charactersCB = {}
        
        self.registerHandler(IN_LINKGROUPS, LinkgroupsHandler(self.linkgroups))
        self.registerHandler(IN_POOLS, PoolsHandler(self.pools))
        self.registerHandler(IN_POOLGROUPS, PoolgroupsHandler(self.poolgroups))
        self.registerHandler(IN_DOORS, DoorsHandler(self.doors))
        self.registerHandler(IN_RESERVATIONS, ReservationsHandler( \
            self.reservations))
        self.registerHandler(IN_SUMMARY_POOLS, SummaryHandler(self.summary))
        self.registerHandler(IN_LINKS, LinkHandler(self.links))

    def registerHandler(self, state, handler):
        self.startElementCB[state] = handler.startElement
        self.endElementCB[state] = handler.endElement
        self.charactersCB[state] = handler.characters

    def startDocument(self):
        self.state = IN_TOP

    def startElement(self, name, attrs):
        if name == 'dCache':
            self.state = IN_TOP
        if name == 'linkgroups' and self.state == IN_TOP:
            self.state = IN_LINKGROUPS
        elif name == 'links' and self.state == IN_TOP:
            self.state = IN_LINKS
        elif name == 'pools' and self.state == IN_TOP:
            self.state = IN_POOLS
        elif name == 'reservations' and self.state == IN_TOP:
            self.state = IN_RESERVATIONS
        elif name == 'poolgroups' and self.state == IN_TOP:
            self.state = IN_POOLGROUPS
        elif name == 'doors' and self.state == IN_TOP:
            self.state = IN_DOORS
        elif name == 'summary' and self.state == IN_TOP:
            self.state = IN_SUMMARY
        elif name == 'pools' and self.state == IN_SUMMARY:
            self.state = IN_SUMMARY_POOLS
        elif self.state == IN_TOP and name == 'cell' and attrs.get('name', '') \
                == 'PoolManager':
            self.state = IN_POOLMANAGER
        elif self.state == IN_POOLMANAGER and name == 'metric' and \
                attrs.get('name', '') == 'release':
            self.state = IN_POOLMANAGER_VERSION
        elif self.state in self.startElementCB:
            self.startElementCB[self.state](name, attrs)

    def endElement(self, name):
        if name == 'linkgroups' and self.state == IN_LINKGROUPS:
            self.state = IN_TOP
        elif name == 'links' and self.state == IN_LINKS:
            self.state = IN_TOP
        elif name == 'pools' and self.state == IN_POOLS:
            self.state = IN_TOP
        elif name == 'reservations' and self.state == IN_RESERVATIONS:
            self.state = IN_TOP
        elif name == 'poolgroups' and self.state == IN_POOLGROUPS:
            self.state = IN_TOP
        elif name == 'doors' and self.state == IN_DOORS:
            self.state = IN_TOP
        elif name == 'pools' and self.state == IN_SUMMARY_POOLS:
            self.state = IN_SUMMARY
        elif name == 'summary' and self.state == IN_SUMMARY:
            self.state = IN_TOP
        elif self.state == IN_POOLMANAGER and name == 'cell':
            self.state = IN_TOP
        elif self.state == IN_POOLMANAGER_VERSION and name == 'metric':
            self.state = IN_TOP
        elif self.state in self.endElementCB:
            self.endElementCB[self.state](name)

    def characters(self, ch):
        if self.state in self.charactersCB:
            self.charactersCB[self.state](ch)
        elif self.state == IN_POOLMANAGER_VERSION:
            self.version = ch.strip()

class ObjectHandler(ContentHandler):

    def __init__(self, objname, useful_metrics):
        self.objname = objname
        self.cur_metric = None
        self.cur_type = None
        self.useful_metrics = useful_metrics

    def startElement(self, name, attrs):
        mname = attrs.get('name', '')
        mtype = attrs.get('type', '')
        if name == 'metric' and mname in self.useful_metrics:
            self.cur_metric = str(mname)
            self.cur_type = str(mtype)

    def characters(self, ch):
        if self.cur_metric == None:
            return
        if self.cur_type == 'boolean':
            getattr(self, self.objname)[self.cur_metric] = ch == 'true'
        elif self.cur_type == 'integer':
            try:
                getattr(self, self.objname)[self.cur_metric] = long(ch)
            except:
                pass
        else:
            getattr(self, self.objname)[self.cur_metric] = str(ch)

    def endElement(self, name):
        if name == 'metric' and self.cur_metric != None:
            self.cur_metric = None

class LinkgroupsHandler(ObjectHandler):

    def __init__(self, lginfo):
        ObjectHandler.__init__(self, 'curlg', ['id', 'onlineAllowed',
            'nearlineAllowed', 'name', 'custodialAllowed', 'outputAllowed',
            'replicaAllowed', 'total', 'free', 'available', 'used', 'reserved'])
        self.lginfo = lginfo
        self.curlg = {'acbrs': ''}
        self.in_auth = False

    def startElement(self, name, attrs):
        if name == 'authorised' and attrs.get('name', ''):
            self.curlg['acbrs'] += '{%s}' % str(attrs['name'])
        ObjectHandler.startElement(self, name, attrs)

    def endElement(self, name):
        ObjectHandler.endElement(self, name)
        if name == 'linkgroup' and 'id' in self.curlg:
            self.lginfo[self.curlg['id']] = self.curlg
            self.curlg = {'acbrs': ''}

class PoolsHandler(ObjectHandler):

    def __init__(self, poolinfo):
        ObjectHandler.__init__(self, 'curpool', ['enabled', 'last-heartbeat',
            'total', 'free', 'used'])
        self.poolinfo = poolinfo
        self.curpool = {}

    def startElement(self, name, attrs):
        if name == 'pool' and attrs.get('name', ''):
            self.curpool['name'] = str(attrs['name'])
        else:
            ObjectHandler.startElement(self, name, attrs)

    def endElement(self, name):
        ObjectHandler.endElement(self, name)
        if name == 'pool' and 'name' in self.curpool:
            self.poolinfo[self.curpool['name']] = self.curpool
            self.curpool = {}

class PoolgroupsHandler(ObjectHandler):

    def __init__(self, pginfo):
        ObjectHandler.__init__(self, 'curpg', ['total', 'free', 'used'])
        self.pginfo = pginfo
        self.curpg = {'pools': sets.Set(), 'links': sets.Set()}

    def startElement(self, name, attrs):
        if name == 'poolgroup' and attrs.get('name', ''):
            self.curpg['name'] = str(attrs['name'])
        elif name == 'poolref' and attrs.get('name', ''):
            self.curpg['pools'].add(str(attrs['name']))
        elif name == 'linkref' and attrs.get('name', ''):
            self.curpg['links'].add(str(attrs['name']))
        else:
            ObjectHandler.startElement(self, name, attrs)

    def endElement(self, name):
        ObjectHandler.endElement(self, name)
        if name == 'poolgroup' and 'name' in self.curpg:
            self.pginfo[self.curpg['name']] = self.curpg
            self.curpg = {'pools': sets.Set(), 'links': sets.Set()}

class DoorsHandler(ObjectHandler):

    def __init__(self, doorinfo):
        ObjectHandler.__init__(self, 'curdoor', ['port', 'update-time',
            'load', 'family', 'engine', 'version', 'FQDN'])
        self.doorinfo = doorinfo
        self.curdoor = {}

    def startElement(self, name, attrs):
        if name == 'door' and attrs.get('name', ''):
            self.curdoor['name'] = str(attrs['name'])
        else:
            ObjectHandler.startElement(self, name, attrs)

    def endElement(self, name):
        ObjectHandler.endElement(self, name)
        if name == 'door' and 'name' in self.curdoor:
            self.doorinfo[self.curdoor['name']] = self.curdoor
            self.curdoor = {}

class ReservationsHandler(ObjectHandler):

    def __init__(self, resinfo):
        ObjectHandler.__init__(self, 'curres', ['id', 'lifetime',
            'access-latency', 'unix', 'description', 'state',
            'retention-policy', 'total', 'free', 'allocated', 'used', 'FQAN',
            'group', 'linkgroupref'])
        self.resinfo = resinfo
        self.curres = {}

    def startElement(self, name, attrs):
        if name == 'reservation' and attrs.get('reservation-id', ''):
            self.curres['id'] = str(attrs['reservation-id'])
        else:
            ObjectHandler.startElement(self, name, attrs)

    def endElement(self, name):
        ObjectHandler.endElement(self, name)
        if name == 'reservation' and 'id' in self.curres:
            self.resinfo[self.curres['id']] = self.curres
            self.curres = {}

class SummaryHandler(ObjectHandler):

    def __init__(self, summaryinfo):
        ObjectHandler.__init__(self, 'summaryinfo', ['total', 'free', 'used'])
        self.summaryinfo = summaryinfo

class LinkHandler(ObjectHandler):

    def __init__(self, linkinfo):
        ObjectHandler.__init__(self, 'curlink', ['cache', 'write', 'read',
            'p2p'])
        self.linkinfo = linkinfo
        self.curlink = {}

    def startElement(self, name, attrs):
        if name == 'link' and attrs.get('name', ''):
            self.curlink['name'] = str(attrs['name'])
        else:
            ObjectHandler.startElement(self, name, attrs)

    def endElement(self, name):
        ObjectHandler.endElement(self, name)
        if name == 'link' and 'name' in self.curlink:
            self.linkinfo[self.curlink['name']] = self.curlink
            self.curlink = {}

def parse_fp(fp):
    handler = InfoProviderHandler()
    parser = make_parser()
    parser.setContentHandler(handler)
    parser.setFeature(feature_external_ges, False)
    try:
        parser.parse(fp)
    except SAXParseException, e:
        log.warning(e)
    return handler

if __name__ == '__main__':
    handler = InfoProviderHandler()
    input_file = sys.argv[1]
    parser = make_parser()
    parser.setContentHandler(handler)
    parser.setFeature(feature_external_ges, False)
    parser.parse(open(input_file, 'r'))
    print handler.linkgroups
    print handler.pools
    print handler.poolgroups
    print handler.doors
    print handler.reservations
    print handler.summary
    print handler.links
    print handler.version
