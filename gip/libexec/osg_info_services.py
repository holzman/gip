#!/usr/bin/python

import os
import re
import sys
import sets
import time
import types
import signal
import urllib2
import httplib
import datetime
import optparse
import threading
import cStringIO
import ConfigParser

from xml.sax.saxutils import XMLGenerator

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
import osg_info_wrapper
import gip_common
import gip_ldap
from gip_logging import getLogger
 
log = getLogger('CEMonUploader')

DEBUG = False


# Sadly, this had to be copy/pasted from gip_common because
# of a slight incompatible fqan normalization.
def matchFQAN_services(fqan1, fqan2):
    """
    Return True if fqan1 matches with fqan2, False otherwise

    fqan1 may actually be more specific than fqan2.  So, if fqan1 is /cms/blah
    and fqan2 is /cms, then there is a match.  If the Role=* for fqan2, the
    value of the Role for fqan1 is ignored.

    FQANs may be of the form:
       - VOMS:<FQAN>
       - VO:<VO Name>
       - <FQAN>
       - <VO>

    @param fqan1: The FQAN we are testing for match
    @param fqan2: The FQAN 
    """
    fqan1 = gip_common.normalizeFQAN(fqan1)
    fqan2 = gip_common.normalizeFQAN(fqan2)
    vog1, vor1 = fqan1.split('Role=')
    vog2, vor2 = fqan2.split('Role=')
    vog_matches = False
    vor_matches = False
    if vor2 == '*':
        vor_matches = True
    elif vor2 == vor1:
        vor_matches = True
    if vog1.startswith(vog2):
        vog_matches = True
    return vog_matches and vor_matches

_match_cache = {}
def matchFQAN(acbr1, acbr2):
    t = (acbr1, acbr2)
    match = _match_cache.get(t, None)
    if match != None:
        return match
    match = matchFQAN_services(acbr1, acbr2)
    _match_cache[t] = match
    return match

class HTTPSHandler2(urllib2.HTTPSHandler):

    def __init__(self, *cert, **kw):
        
        ext_key = kw.setdefault('key_file',
            '/etc/grid-security/containerkey.pem')
        ext_cert = kw.setdefault('certificate_file',
            '/etc/grid-security/containercert.pem')

        class HTTPSConnection2(httplib.HTTPSConnection):

            def __init__(self, host):
                httplib.HTTPSConnection.__init__(self, host)
                self.key_file = ext_key 
                self.cert_file = ext_cert
        self.conn_class = HTTPSConnection2
        del kw['key_file']
        del kw['certificate_file']
        urllib2.HTTPSHandler.__init__(self, *cert, **kw)
 
    def https_open(self, req):
        return self.do_open(self.conn_class, req)

    https_request = urllib2.HTTPSHandler.do_request_


def filter_by_class(entries, objectClass):
    filter = []
    for entry in entries:
        if objectClass in entry.objectClass:
            filter.append(entry)
    return filter


def join_FK(item, join_list, join_attr, join_fk_name="ForeignKey"):
    if item.multi:
        item_fks = item.glue[join_fk_name]
        for item_fk in item_fks:
            for entry in join_list:
                if entry.multi:
                    for val in entry.glue[join_attr]:
                        test_val = "Glue%s=%s" % (join_attr, val)
                        if test_val == item_fk:
                            return entry
                else:
                    test_val = "Glue%s=%s" % (join_attr, entry.glue[join_attr])
                    if test_val == item_fk:
                        return entry
    else:
        item_fk = item.glue[join_fk_name]
        for entry in join_list:
            if entry.multi:
                for val in entry.glue[join_attr]:
                    test_val = "Glue%s=%s" % (join_attr, val)
                    if test_val == item_fk:
                        return entry
            else:
                test_val = "Glue%s=%s" % (join_attr, entry.glue[join_attr])
                if test_val == item_fk:
                    return entry
    raise ValueError("Unable to find matching entry in list.")


def determine_ses(ce, all_cese, all_cese_se):
    # Determine CESE binds, if any
    if ce.multi:
        unique = ce.glue['CEUniqueID'][0]
    else:
        unique = ce.glue['CEUniqueID']
    adjacent_ses = []
    se_to_cese = {}
    for cese in all_cese:
        if cese.multi and unique in cese.glue['CESEBindGroupCEUniqueID']:
            for se in cese.glue['CESEBindGroupSEUniqueID']:
                adjacent_ses.append(se)
        elif not cese.multi and unique == cese.glue['CESEBindGroupCEUniqueID']:
            adjacent_ses.append(cese.glue['CESEBindGroupSEUniqueID'])
            se_to_cese[se] = cese
    for cese in all_cese_se:
        for se in adjacent_ses:
            if cese.multi and unique in cese.glue['CESEBindCEUniqueID'] \
                    and se in cese.glue['CESEBindSEUniqueID']:
                se_to_cese[se] = cese
            elif not cese.multi and unique==cese.glue['CESEBindCEUniqueID']\
                    and se in cese.glue['CESEBindSEUniqueID']:
                se_to_cese[se] = cese
    return adjacent_ses, se_to_cese


ap_multi_attributes = ['SEAccessProtocolEndpoint', 'SEAccessProtocolVersion',
    'SEAccessProtocolLocalID', 'SEAccessProtocolSupportedSecurity',
    'SEAccessProtocolMaxStreams']
drop_attrs = ['GlueForeignKey', 'GlueSiteDescription', 'GlueSiteLocation',
    'GlueSiteWeb', 'GlueSiteSponsor', 'GlueSiteOtherInfo', 'GlueChunkKey',
    'GlueCESEBindSEUniqueID', 'GlueCESEBindCEUniqueID',
    'GlueCESEBindCEAccesspoint']


class ClassAdSink(object):

    def emit(self, classad):
        raise NotImplementedError()

    def run(self):
        pass


class ClassAdPrinter(ClassAdSink):

    def emit(self, results):
        output = []
        keys = results.keys()
        keys.sort()
        for key in keys:
            val = results[key]
            if key in drop_attrs:
                continue
            if isinstance(val, types.IntType):
                output.append('%s = %i;' % (key, val))
            else:       
                output.append('%s = "%s";' % (key, str(val)))
        print "\n".join(output) + "\n"


class CEMonMessageProducer(ClassAdSink):

    def __init__(self, hosts, certificate_file=None, key_file=None):
        super(CEMonMessageProducer, self).__init__()
        self.certificate = certificate_file
        self.key = key_file
        if not hasattr(self, 'default_url'):
            self.default_url = ''
        self.endpoints = []
        for host in hosts:
            if host.find('://') >= 0:
                self.endpoints.append(host)
            else:
                if len(host.split(':')) != 2:
                    host = '%s:14001' % host
                self.endpoints.append('http://%s%s' % (host, self.default_url))

    def generate(self, messages, producer="OSG CE Sensor"):
        self.encoding = 'UTF-8'
        output = cStringIO.StringIO()
        gen = XMLGenerator(output, self.encoding)
        gen.startDocument()
        gen.startPrefixMapping('soapenv',
            'http://schemas.xmlsoap.org/soap/envelope/')
        gen.startElementNS(('http://schemas.xmlsoap.org/soap/envelope/',
            'Envelope'), 'Envelope', {})
        gen.characters('\n ')
        gen.startElementNS(('http://schemas.xmlsoap.org/soap/envelope/',
            'Body'), 'Body', {})
        gen.characters('\n  ')
        gen.startElement('Notify', {'xmlns': \
            'http://glite.org/ce/monitorapij/ws'})
        gen.characters('\n   ')
        gen.startPrefixMapping('glite', 'http://glite.org/ce/monitorapij/types')
        gen.startElementNS(('http://glite.org/ce/monitorapij/types',
            'Notification'), 'Notification', {("http://glite.org/ce/" \
            "monitorapij/types", 'ConsumerURL'): \
            'https://osg-ress-4.fnal.gov:8443/ig/services/CEInfoCollector'})
        gen.characters('\n    ')
        gen.startElementNS(('http://glite.org/ce/monitorapij/types',
            'ExpirationTime'), 'ExpirationTime', {})
        gen.characters((datetime.datetime.now() + datetime.timedelta(7, 0)).\
            strftime('%Y-%m-%dT%H:%M:%SZ'))
        gen.endElementNS(('http://glite.org/ce/monitorapij/types',
            'ExpirationTime'), 'ExpirationTime')
        gen.characters('\n    ')
        gen.startElementNS(('http://glite.org/ce/monitorapij/types', 'Topic'),
            'Topic', {})
        gen.characters('\n     ')
        gen.startElementNS(('http://glite.org/ce/monitorapij/types', 'Name'),
            'Name', {})
        gen.characters('OSG_CE')
        gen.endElementNS(('http://glite.org/ce/monitorapij/types', 'Name'),
            'Name')
        gen.characters('\n     ')
        gen.startElementNS(('http://glite.org/ce/monitorapij/types', 'Dialect'),
            'Dialect', {})
        gen.startElementNS(('http://glite.org/ce/monitorapij/types', 'Name'),
            'Name', {})
        gen.characters('OLD_CLASSAD')
        gen.endElementNS(('http://glite.org/ce/monitorapij/types', 'Name'),
            'Name')
        gen.endElementNS(('http://glite.org/ce/monitorapij/types', 'Dialect'),
            'Dialect')
        gen.characters('\n    ')
        gen.endElementNS(('http://glite.org/ce/monitorapij/types', 'Topic'),
            'Topic')
        gen.characters('\n    ')
        gen.startElementNS(('http://glite.org/ce/monitorapij/types', 'Event'),
            'Event', {})
        gen.characters('\n    ')
        gen.startElementNS(('http://glite.org/ce/monitorapij/types', 'ID'),
            'ID', {})
        gen.characters('-1')
        gen.endElementNS(('http://glite.org/ce/monitorapij/types', 'ID'), 'ID')
        gen.characters('\n     ')
        gen.startElementNS(('http://glite.org/ce/monitorapij/types',
            'Timestamp'), 'Timestamp', {})
        gen.characters(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
        gen.endElementNS(('http://glite.org/ce/monitorapij/types',
            'Timestamp'), 'Timestamp')
        gen.characters('\n    ')
        
        for ad in messages:
            gen.startElementNS(('http://glite.org/ce/monitorapij/types',
                'Message'), 'Message', {})
            gen.characters(ad)
            gen.endElementNS(('http://glite.org/ce/monitorapij/types',
                'Message'), 'Message')
        gen.characters('\n     ')
        gen.startElementNS(('http://glite.org/ce/monitorapij/types',
        'Producer'), 'Producer', {})
        gen.characters(producer)
        gen.endElementNS(('http://glite.org/ce/monitorapij/types', 'Producer'),
            'Producer')
        gen.characters('\n    ')
        gen.endElementNS(('http://glite.org/ce/monitorapij/types', 'Event'),
            'Event')
        gen.characters('\n   ')
        gen.endElementNS(('http://glite.org/ce/monitorapij/types', 
            'Notification'), 'Notification')
        gen.characters('\n  ')
        gen.endPrefixMapping('glite')
        gen.endElement('Notify')
        gen.characters('\n ')
        gen.endElementNS(('http://schemas.xmlsoap.org/soap/envelope/', 
            'Body'), 'Body')
        gen.characters('\n')
        gen.endElementNS(('http://schemas.xmlsoap.org/soap/envelope/',
            'Envelope'), 'Envelope')
        gen.endPrefixMapping('soapenv')
        gen.endDocument()
        results = output.getvalue()
        log.debug("CEMon notification results:\n%s" % results)
        return results

    def run(self):
        if not hasattr(self, 'encoded_output'):
            return
        all_threads = []
        for endpoint in self.endpoints:
            t = threading.Thread(target=self._thread_target, args=(endpoint,))
            t.setName('Uploader for endpoint %s' % endpoint)
            t.setDaemon(True)
            t.start()
            all_threads.append(t)
        for t in all_threads:
            t.join()

    def _thread_target(self, endpoint):
        try:
            req = urllib2.Request(endpoint, self.encoded_output)
            req.add_header("SOAPAction", 
                '"http://glite.org/ce/monitorapij/ws/Notify"')
            req.add_header("Cache-Control", "no-cache")
            req.add_header("Pragma", "no-cache")
            req.add_header("Accept", "application/soap+xml, " \
                "application/dime, multipart/related, text/*")
            log.info("Uploading output to %s" % endpoint)
            kw = {}
            if self.certificate:
                kw['certificate_file'] = self.certificate
            if self.key:
                kw['key_file'] = self.key
            auth_handler = HTTPSHandler2(**kw)
            opener = urllib2.build_opener(auth_handler)
            output = opener.open(req).read()
            # print output
        except Exception, e:
            msg = "\nException occured for endpoint: %s \n\n%s" % (endpoint, str(e))
            log.exception(msg)
            raise


class BdiiSender(CEMonMessageProducer):

    def __init__(self, hosts, **kw):
        self.default_url = '/'
        super(BdiiSender, self).__init__(hosts, **kw)
        self.ads = []
        log.info("BDII info destinations: %s" % ', '.join(self.endpoints))

    def emit(self, results):
        self.ads.append(results)

    def run(self):
        messages = ['\n'.join([ldap.to_ldif() for ldap in self.ads])]
        self.encoded_output = self.generate(messages)
        super(BdiiSender, self).run()


class ClassAdSender(CEMonMessageProducer):

    def __init__(self, hosts, **kw):
        self.default_url = '/ig/services/CEInfoCollector'
        super(ClassAdSender, self).__init__(hosts, **kw)
        self.ads = []
        log.info("ClassAd destinations: %s" % ', '.join(self.endpoints))

    def emit(self, classad):
        self.ads.append(classad)

    def run(self):
        messages = []
        
        for ad in self.ads:
            out = []
            keys = ad.keys()
            keys.sort()
            for key in keys:
                val = ad[key]
                if key in drop_attrs:
                    continue
                if isinstance(val, types.IntType):
                    out.append('%s = %i;' % (key, val))
                elif val in ("TRUE", "FALSE"):
                    out.append("%s = %s" % (key, val))
                else:
                    out.append('%s = "%s";' % (key, str(val)))
            message = "[\n" + "\n        ".join(out) + "\n\n]"
            messages.append(message)
        log.info("Generated %i ClassAds for endpoints %s" % (len(self.ads),
            ", ".join(self.endpoints)))
        output = self.generate(messages)
        self.encoded_output = output
        super(ClassAdSender, self).run()


class ClassAdEmitter(object):

    def __init__(self):
        self.emitters = []

    def add_emitter(self, emit):
        self.emitters.append(emit)

    def run(self):
        for emitter in self.emitters:
            emitter.run()

    def add_to_results(self, entry, results):
        for glue, val in entry.glue.items():
            key = "Glue" + glue
            try:
                results[key] = int(','.join(val))
            except:
                results[key] = ','.join([str(i) for i in val])

        for glue, val in entry.nonglue.items():
            try:
                results[glue] = int(','.join(val))
            except:
                results[glue] = ','.join([str(i) for i in val])

    def add_aps(self, aps, results):
        for ap in aps:
            self.add_to_results(ap, results)
        for attr in ap_multi_attributes:
            attr_val = []
            for ap in aps:
                attr_val.append(str(ap.glue[attr][0]))
            results["Glue" + attr] = ",".join(attr_val)

    def add_software(self, software, results):
        pass

    def can_access(self, obj1_acbrs, obj2, attribute):
        """
        Return true if any of the entries in the obj1_acbrs are allowed to
        access obj2 (using the glue attribute named "attribute")
        """
        obj2_acbrs = obj2.glue.get(attribute, [])
        if not obj2_acbrs:
            return False
        for acbr1 in obj1_acbrs:
            for acbr2 in obj2_acbrs:
                if matchFQAN(acbr1, acbr2):
                    return True
        return False

    _match_view_cache = {}
    def can_match_view(self, ce, voview=None, sa=None, voinfo=None):
        voview_acbr = ce.glue['CEAccessControlBaseRule']
        if voview:
            voview_acbr = voview.glue['CEAccessControlBaseRule']
        t = (ce.__hash__(), voview_acbr, sa.__hash__(), voinfo.__hash__())
        if t in self._match_view_cache:
            return self._match_view_cache[t]
        if self.can_access(voview_acbr, sa, "SAAccessControlBaseRule"):
            if voinfo:
                if self.can_access(voview_acbr, voinfo,
                        "VOInfoAccessControlBaseRule"):
                    self._match_view_cache[t] = True
                else:
                    self._match_view_cache[t] = False
            else:
                self._match_view_cache[t] = True
        else:
            self._match_view_cache[t] = False
        return self._match_view_cache[t]

    def filter_voviews(self, ce, voviews, sa, voinfo=None):
        results = [i for i in voviews if self.can_match_view(ce, i, sa, voinfo)]
        if results:
            return results
        return None

    def emit(self, site=None, cluster=None, ce=None, voview=None, software=None,
            aps=None, service=None, se=None, voinfo=None, sa=None,
            subcluster=None, cese=None, cp=None, **kw):

        if not site or not cluster or not ce:
            return

        results = {}
        # Add mandatory entities
        self.add_to_results(ce, results)
        self.add_to_results(site, results)
        self.add_to_results(cluster, results)

        # Work on optional entities
        voview_acbr = ce.glue['CEAccessControlBaseRule']
        secondary_ce_acbrs = []
        if voview:
            self.add_to_results(voview, results)
            voview_acbr = voview.glue['CEAccessControlBaseRule']

        # Add the optional SA/VOInfo for this information.  Note that if the
        # SA or VOInfo exists and the corresponding CE/VOView can't access it,
        # we just return.
        if sa and not self.can_match_view(ce, voview, sa, voinfo):
            return

        if voview:
            # Go through each CE ACBR and we'll add the ACBR to the final
            # ClassAd if the CE ACBR is different from the VOView ACBR and the
            # CE ACBR can access the VOView.
            # Use case: CE ACBR has VO:cms and VOMS:/cms/Role=t1access, while
            # there only exists a VOView for VO:cms
            for ce_acbr in ce.glue['CEAccessControlBaseRule']:
                if self.can_access([ce_acbr], voview, 
                        "CEAccessControlBaseRule"):
                    if ce_acbr not in voview_acbr:
                        secondary_ce_acbrs.append(ce_acbr)
        if service:
            self.add_to_results(service, results)
        if cp:
            self.add_to_results(cp, results)
        if se:
            self.add_to_results(se, results)
        if cese:
            self.add_to_results(cese, results)
        if subcluster:
            self.add_to_results(subcluster, results)

        if DEBUG and (subcluster.glue["SubClusterUniqueID"][0] != \
                "fnpcosg1.fnal.gov-FNAL_FERMIGRID"):
            return

        # If there are secondary ACBRs for this ClassAd, add them now.
        if secondary_ce_acbrs:
            key = "GlueCEAccessControlBaseRule"
            prev_results = []
            if key in results:
                prev_results = results[key].split(",")
            results[key] = ','.join(prev_results + [str(i) for i in \
                secondary_ce_acbrs])

        # Separate functions handle software and APs
        if aps:
            self.add_aps(aps, results)
        if software:
            self.add_software(software, results)

        # Finally, emit as a ClassAd
        self.emit_classad(results)

    def emit_classad(self, results):
        for emitter in self.emitters:
            emitter.emit(results)

    def emit_ce(self, subclusters, ce, voviews, **kw):
        if subclusters:
            for subcluster in subclusters:
                if voviews:
                    for voview in voviews:
                        self.emit(ce=ce, voview=voview, subcluster=subcluster,
                            **kw)
                else:
                    self.emit(ce=ce, subcluster=subcluster, **kw)
        else:
            if voviews:
                for voview in voviews:
                    self.emit(ce=ce, voview=voview, **kw)
            else:
                self.emit(ce=ce, **kw)

    def sort_aps(self, aps):
        """
        Take a list of AccessProtocol entities and sort them by the protocol
        type.

        Returns a dictionary whose keys are the protocol type (gsiftp, dcap,
        etc) and value a list of access protocols of that type.
        """
        results = {}
        type = None
        for ap in aps:
            # Really, we should have one ClassAd per AccessProtocol type
            # Adding the next two lines for precise matching of CEMon
            if not type:
                type = ap.glue.get('SEAccessProtocolType', None)
            if not type:
                continue
            ap_list = results.setdefault(type, [])
            ap_list.append(ap)
        return results

    def emit_se(self, subclusters, ce, voviews, **kw):
        aps = kw['aps']
        services = kw['services']
        aps = self.sort_aps(aps)
        if services:
            for service in services:
                kw['service'], kw['cp'] = service
                # For CEMon compatibility, do not advertise SRMv2.
                if service[0].glue.get("ServiceType", ["Unknown"])[0] == "SRM" \
                        and service[0].glue.get("ServiceVersion", [""])[0] \
                        .find("1") >= 0:
                    continue
                if aps:
                    for ap_type in aps.values():
                        kw['aps'] = ap_type
                        self.emit_ce(subclusters, ce, voviews, **kw)
                else:
                    self.emit_ce(subclusters, ce, voviews, **kw)
        else:
            if aps:
                for ap_type in aps.values():
                    kw['aps'] = ap_type
                    self.emit_ce(subclusters, ce, voviews, **kw)
            else:
                self.emit_ce(subclusters, ce, voviews, **kw)


def map_to_list(key_class, values_class, join):
    results = {}
    for val in values_class:
        try:
            key = join_FK(val, key_class, join, join_fk_name="ChunkKey")
            val_list = results.setdefault(key, [])
            val_list.append(val)
        except:
            print >> sys.stderr, "Unable to find matching key for:\n%s" % \
                val.to_ldif()
            continue
    return results


def set_logging(level):
    py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3
    if not py23:
        return
    logging = __import__("logging")
    LEVELS = {'debug': logging.DEBUG,
              'info': logging.INFO,
              'warning': logging.WARNING,
              'error': logging.ERROR,
              'critical': logging.CRITICAL}
    level = level.lower()
    log = logging.getLogger()
    log.setLevel(LEVELS.get(level, logging.NOTSET))
    for handler in log.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setLevel(LEVELS.get(level, logging.NOTSET))


split_re = re.compile('\s*;?,?\s*')
def configure_emitter():

    parser = optparse.OptionParser()
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
        default=False)
    parser.add_option("-g", "--ignore_config", action="store_false",
        dest="config", help="Do not use the settings in the OSG config files.",
        default=True)
    parser.add_option("-b", "--bdii", dest="bdii", help="BDII servers to send" \
        " this data to.", action="append")
    parser.add_option("-r", "--ress", dest="ress", help="ReSS servers to send" \
        " this data to.", action="append")
    parser.add_option("-c", "--certificate", dest="certificate", help="Cert" \
        "ificate file to use.", default=None)
    parser.add_option("-i", "--uri", dest="uri", help="URI for input data.",
        default=None)
    parser.add_option("-k", "--keyfile", dest="key", help="Key file to use.",
        default=None)
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true",
        default=False, help="Remove all output except fatal errors.")
    parser.add_option("-d", "--dryrun", dest="dryrun", action="store_true",
        default=False, help="Do not actually send data upstream")
    parser.add_option("-t", "--timeout", dest="timeout", type="int", default=0,
        help="Force the script to die after a certain period")
    parser.add_option("--use_cache", dest="cache", default=False,
        action="store_true", help="Use the GIP's built-in caching mechanism")
    options, args = parser.parse_args()
    sys.argv = []
    if not options.config:
        # If we are ignoring the config, we can ignore exceptions and create
        # an empty configuration object.
        try:
            cp = gip_common.config()
        except:
            cp = ConfigParser.ConfigParser()
            cp.add_section("gip")
    else:
        cp = gip_common.config()

    # Set up the alarm to automatically kill a runaway script
    try:
        options.timeout = int(options.timeout)
    except:
        options.timeout = 0
    if options.timeout > 0:
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.alarm(options.timeout)
        cp.set("gip", "response", options.timeout)

    # Determine whether we should use the cache
    if options.cache:
        cp.set("gip", "flush_cache", "False")
    else:
        cp.set("gip", "flush_cache", "True")

    # Set logging verbosity
    set_logging("info")
    if options.quiet:
        set_logging("error")

    cae = ClassAdEmitter()

    if options.verbose:
        set_logging("debug")
        cae.add_emitter(ClassAdPrinter())

    kw = {}
    if options.key:
        kw['key_file'] = options.key
    if options.certificate:
        kw['certificate_file'] = options.certificate

    if not options.ress:
        options.ress = []
    if not options.bdii:
        options.bdii = []

    if options.config:
        bdii_endpoints = gip_common.cp_get(cp, "gip", "bdii_endpoints", "")
        ress_endpoints = gip_common.cp_get(cp, "gip", "ress_endpoints", "")
        for endpoint in split_re.split(ress_endpoints):
            options.ress.append(endpoint)
        for endpoint in split_re.split(bdii_endpoints):
            options.bdii.append(endpoint)

    if options.ress:
        log.info("ReSS endpoints: %s" % ", ".join(options.ress))
        cae.add_emitter(ClassAdSender(options.ress, **kw))
    else:
        log.info("No ReSS endpoints configured.")

    if options.bdii:
        log.info("BDII endpoints: %s" % ", ".join(options.bdii))
        bdii = BdiiSender(options.bdii, **kw)
    else:
        log.info("No BDII endpoints configured.")
        bdii = None
    
    return cae, bdii, options.uri, options.dryrun, options.timeout, cp


def main():
    cae, bdii, uri, dryrun, timeout, cp = configure_emitter()
    if not uri:
        t1 = -time.time()
        entries = osg_info_wrapper.main(cp=cp, return_entries=True)
        t1 += time.time()
        # Reset the signal handler which the info wrapper clobbers
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        remaining_time = int(timeout-t1)
        if remaining_time <= 0 and timeout > 0:
            remaining_time = 1
        signal.alarm(remaining_time)
    else:
        fd = urllib2.urlopen(uri)
        entries = gip_ldap.read_ldap(fd, multi=True)
    upload(cae, bdii, entries, dryrun=dryrun)


def upload(cae, bdii, entries, dryrun=False):
    """
    Converts the list of LdapData objects (entries) to ClassAds.  Submit
    the list of resulting class ads and original LDAP to the respective
    servers.

       * cae: ClassAdEmitter object
       * bdii: BdiiSender object
       * entries: list of LdapData objects.
    """
    all_sites = filter_by_class(entries, 'GlueSite')
    all_ces = filter_by_class(entries, 'GlueCE')
    all_voviews = filter_by_class(entries, 'GlueVOView')
    all_voinfos = filter_by_class(entries, 'GlueVOInfo')
    all_ses = filter_by_class(entries, 'GlueSE')
    all_sas = filter_by_class(entries, 'GlueSA')
    all_clusters = filter_by_class(entries, 'GlueCluster')
    all_subclusters = filter_by_class(entries, 'GlueSubCluster')
    all_sub = filter_by_class(entries, "GlueSubCluster")
    all_cese = filter_by_class(entries, "GlueCESEBindGroup")
    all_cese_se = filter_by_class(entries, "GlueCESEBind")
    all_services = filter_by_class(entries, "GlueService")
    all_cps = filter_by_class(entries, "GlueSEControlProtocol")
    all_aps = filter_by_class(entries, "GlueSEAccessProtocol")

    # Map the SE Unique ID to the SE object:
    id_to_se = {}
    for se in all_ses:
        id_to_se[se.glue['SEUniqueID'][0]] = se
    log.info("All SEs found: %s" % ', '.join(id_to_se.keys()))

    # Determine the SE -> CP list mapping
    se_to_cps = {}
    for cp in all_cps:
        try:
            se = join_FK(cp, all_ses, "SEUniqueID", join_fk_name="ChunkKey")
            cp_list = se_to_cps.setdefault(se, [])
            cp_list.append(cp)
        except:
            print >> sys.stderr, "Unable to find SE for SRM; skipping\n%s" % \
                cp.to_ldif()
            continue

    # Map CPs to service
    cp_to_service = {}
    for service in all_services:
        for cp in all_cps:
            if service.glue['ServiceURI'][0] == cp.glue[\
                    'SEControlProtocolEndpoint'][0]:
                cp_to_service[cp] = service

    # Map SE to services:
    se_to_services = {}
    for se in all_ses:
        for cp in se_to_cps.get(se, []):
            if cp in cp_to_service:
                service_list = se_to_services.setdefault(se, [])
                service_list.append((cp_to_service[cp], cp))

    # Map SE to SAs
    se_to_sas = map_to_list(all_ses, all_sas, "SEUniqueID")

    # Map SE to APs
    se_to_aps = map_to_list(all_ses, all_aps, "SEUniqueID")

    # Map (SE, SA) to VOInfos
    sesa_to_voinfos = {}
    for voinfo in all_voinfos:
        for se, sas in se_to_sas.items():
            try:
                matched = False
                for key in voinfo.glue['ChunkKey']:
                    val = "GlueSEUniqueID=%s" % se.glue['SEUniqueID'][0]
                    if key == val:
                        matched = True
                        break
                if not matched:
                    continue
                sa = join_FK(voinfo, sas, "SALocalID", join_fk_name="ChunkKey")
                voinfo_list = sesa_to_voinfos.setdefault((se, sa), [])
                voinfo_list.append(voinfo)
            except:
                #print >> sys.stderr, "Unable to find SA for VOInfo;" \
                #    " skipping\n%s" % voinfo.to_ldif()
                continue

    # Map CE to VOViews
    ce_to_voviews = map_to_list(all_ces, all_voviews, "CEUniqueID")

    # Map Cluster to Subclusters
    cluster_to_subclusters = map_to_list(all_clusters, all_subclusters,
        "ClusterUniqueID")

    for ce in all_ces:
        if DEBUG and (ce.glue['CEUniqueID'][0] != \
                "fnpcosg1.fnal.gov:2119/jobmanager-condor-default"):
            continue

        log.info("Creating ClassAds related to CE %s" % ce.glue['CEUniqueID'])
        # Adjoined cluster and site are required.
        try:
            cluster = join_FK(ce, all_clusters, "ClusterUniqueID")
            site = join_FK(cluster, all_sites, "SiteUniqueID")
        except:
            print >> sys.stderr, "Unable to find cluster/site for CE;" \
                " skipping\n%s" % ce.to_ldif()
            continue

        # Adjacent subclusters
        subclusters = cluster_to_subclusters.get(cluster, [])

        # Determine close SEs, if any
        try:
            adjacent_ses, seid_to_cese = determine_ses(ce, all_cese,
                all_cese_se)
        except:
            raise
            adjacent_ses, seid_to_cese = [], []

        # Determine VOViews on this CE
        voviews = ce_to_voviews.get(ce, [])

        # Here's all the nested logic to print out the the class ad even when
        # various pieces are missing.
        # (thinking about better ways to do this)
        ce_kw = {'site': site, 'cluster': cluster}
        #it = len(cae.emitters[0].ads)
        if adjacent_ses:
            # All SEs
            for se in adjacent_ses:
                kw = dict(ce_kw)
                #if DEBUG and (se != "fndca1.fnal.gov"):
                #    continue

                log.info("\tSE adjacent to CE: %s" % se)
                try:
                    kw['se'] = id_to_se[se]
                except:
                    log.info("\tSE not advertised by this CE: %s" % se)
                    continue

                adjacent_sas = se_to_sas.get(kw['se'], [])
                kw['aps'] = se_to_aps.get(id_to_se[se], [])
                kw['services'] = se_to_services.get(kw['se'], [])
                if se in seid_to_cese:
                    kw['cese'] = seid_to_cese[se]
                # All SAs
                if adjacent_sas:
                    for sa in adjacent_sas:
                        if DEBUG and (sa.glue["SALocalID"][0] != \
                                "readPools:custodial:nearline"):
                            continue

                        log.info("\t\tSA adjacent to SE: %s" % \
                            sa.glue['SALocalID'])
                        kw['sa'] = sa
                        voinfos = sesa_to_voinfos.get((id_to_se[se], sa), [])

                        if DEBUG:
                            it = len(cae.emitters[-1].ads)
                        if voinfos:
                            # All VOInfos
                            for voinfo in voinfos:
                                kw['voinfo'] = voinfo
                                my_voviews = cae.filter_voviews(ce, voviews, sa, voinfo)
                                if DEBUG and (voinfo.glue[\
                                        'VOInfoAccessControlBaseRule'][0] != \
                                        'atlas'):
                                    continue
                                if my_voviews:
                                    cae.emit_se(subclusters, ce, my_voviews, **kw)
                        else:
                            if DEBUG:
                                print "Emitting a SA without a VOInfo"
                            cae.emit_se(subclusters, ce, voviews, **kw)
                        if DEBUG:
                            print len(cae.emitters[-1].ads) - it
                else:
                    log.info("\t\tNo SA adjacent to SE.")
                    cae.emit_se(subclusters, ce, voviews, **kw)
        else:
            cae.emit_ce(subclusters, ce, voviews, **ce_kw)
        #print "Ident: ", ce.glue["CEUniqueID"][0], len(cae.emitters[0].ads) - it

    if not dryrun:
        cae.run()

    if bdii:
        for entry in entries:
            bdii.emit(entry)
        if not dryrun:
            bdii.run()


if __name__ == '__main__':
    main()

