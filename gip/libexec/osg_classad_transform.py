#!/usr/bin/python

import sys
import types
import datetime
import optparse
import cStringIO

from xml.sax.saxutils import XMLGenerator

import osg_info_wrapper

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

def determine_ses(ce, all_cese):
    # Determine CESE binds, if any
    if ce.multi:
        unique = ce.glue['CEUniqueID'][0]
    else:
        unique = ce.glue['CEUniqueID']
    adjacent_ses = []
    for cese in all_cese:
        if cese.multi and unique in cese.glue['CESEBindGroupCEUniqueID']:
            for se in cese.glue['CESEBindGroupSEUniqueID']:
                 adjacent_ses.append(se)
        elif not cese.multi and unique == cese.glue['CESEBindGroupCEUniqueID']:
            adjacent_ses.append(cese.glue['CESEBindGroupSEUniqueID'])
    return adjacent_ses

ap_multi_attributes = ['SEAccessProtocolEndpoint', 'SEAccessProtocolVersion',
    'SEAccessProtocolLocalID', 'SEAccessProtocolSupportedSecurity',
    'SEAccessProtocolMaxStreams']
drop_attrs = ['GlueForeignKey']


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

    def __init__(self, host):
        super(CEMonMessageProducer, self).__init__()
        self.host = host

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
            'Notification'), 'Notification', {(None, 'ConsumerURL'): \
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
        return output.getvalue()

class BdiiSender(ClassAdSink, CEMonMessageProducer):

    def __init__(self, host):
        super(BdiiSender, self).__init__(host)
        self.ads = []

    def emit(self, results):
        self.ads.append(results)

    def run(self):
        messages = ['\n'.join([ldap.to_ldif() for ldap in self.ads])]
        output = self.generate(messages)
        print output

class ClassAdSender(ClassAdSink, CEMonMessageProducer):

    def __init__(self, host):
        super(ClassAdSender, self).__init__(host)
        self.ads = []

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
                else:       
                    out.append('%s = "%s";' % (key, str(val)))
            message = "[\n" + "\n        ".join(out) + "\n\n]"
            messages.append(message)
        output = self.generate(messages)
        print output

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
            self.add_to_results(self, ap, results)
        for attr in ap_multi_attributes:
            attr_val = []
            for ap in aps:
                attr_val.append(str(ap.glue[attr][0]))
            results["Glue" + attr_val] = ",".join(attr_val)

    def add_software(self, software, results):
        pass

    def emit(self, site=None, cluster=None, ce=None, voview=None, software=None,
            aps=None, service=None, se=None, voinfo=None, sa=None,
            subcluster=None, **kw):

        if not site or not cluster or not ce:
            return

        results = {}
        # Add mandatory entities
        self.add_to_results(ce, results)
        self.add_to_results(site, results)
        self.add_to_results(cluster, results)

        # Work on optional entities
        voview_acbr = ce.glue['CEAccessControlBaseRule']
        if voview:
            self.add_to_results(voview, results)
            voview_acbr = voview.glue['CEAccessControlBaseRule']
        if service:
            self.add_to_results(service, results)
        if se:
            self.add_to_results(se, results)
        if subcluster:
            self.add_to_results(subcluster, results)

        # Add the optional SA/VOInfo for this information.  Note that if the
        # SA or VOInfo exists and the corresponding CE/VOView can't access it,
        # we just return.
        if sa and can_access(voview_acbr, sa, "SAAccessControlBaseRule"):
            self.add_to_results(sa, results)
            if voinfo and can_access(vovoview_acbr, voinfo,
                    "VOInfoAccessControlBaseRule"):
                self.add_to_results(voinfo, results)
            elif voinfo:
                return
        elif sa:
            return

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
        for ap in aps:
            type = ap.glue.get('SEAccessProtocolType', None)
            if not type:
                continue
            ap_list = results.setdefault(type, [])
            ap_list.append(ap)
        return results

    def emit_se(self, subclusters, ce, voviews, **kw):
        aps = kw['aps']
        aps = self.sort_aps(aps)
        services = kw['services']
        if services:
            for service in services:
                kw['service'] = service
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

def configure_emitter():

    cae = ClassAdEmitter()

    parser = optparse.OptionParser()
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose")
    parser.add_option("-b", "--bdii", dest="bdii", help="BDII servers to send" \
        " this data to.", action="append")
    parser.add_option("-r", "--ress", dest="ress", help="ReSS servers to send" \
        " this data to.", action="append")
    options, args = parser.parse_args()

    #if options.verbose:
    #cae.add_emitter(ClassAdPrinter())

    #if options.ress:
    cae.add_emitter(ClassAdSender())

    #if options.bdii:
    bdii = BdiiSender()
    #else:
    #bdii = None
    
    return cae, bdii

def main():

    cae, bdii = configure_emitter()

    entries = osg_info_wrapper.main(return_entries=True)
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
    all_services = filter_by_class(entries, "GlueService")
    all_cps = filter_by_class(entries, "GlueSEControlProtocol")
    all_aps = filter_by_class(entries, "GlueSEAccessProtocol")

    # Map the SE Unique ID to the SE object:
    id_to_se = {}
    for se in all_ses:
        id_to_se[se.glue['SEUniqueID'][0]] = se

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
                service_list.append(cp_to_service[cp])

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
            adjacent_ses = determine_ses(ce, all_cese)
        except:
            raise
            adjacent_ses = []

        # Determine VOViews on this CE
        voviews = ce_to_voviews.get(ce, [])

        # Here's all the nested logic to print out the the class ad even when
        # various pieces are missing.
        # (thinking about better ways to do this)
        kw = {'site': site, 'cluster': cluster}
        print >> sys.stderr, "Adjacent SEs %s." % ", ".join(adjacent_ses)
        if adjacent_ses:
            # All SEs
            for se in adjacent_ses:
                adjacent_sas = se_to_sas.get(se, [])
                kw['se'] = id_to_se[se]
                kw['aps'] = se_to_aps.get(se, [])
                kw['services'] = se_to_services.get(se, [])
                # All SAs
                if adjacent_sas:
                    for sa in adjacent_sas:
                        kw['sa'] = sa
                        voinfos = sesa_to_voinfos.get((se, sa), [])
                        if voinfos:
                            # All VOInfos
                            for voinfo in voinfos:
                                kw['voinfo'] = voinfo
                                cae.emit_ce(subclusters, ce, voviews, **kw)
                        else:
                            cae.emit_se(subclusters, ce, voviews, **kw)
                else:
                    cae.emit_se(subclusters, ce, voviews, **kw)
        else:
            cae.emit_ce(subclusters, ce, voviews, **kw)

    cae.run()

    if bdii:
        for entry in entries:
            bdii.emit(entry)
        bdii.run()

if __name__ == '__main__':
    main()

