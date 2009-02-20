
"""
Implementation of a CE for a forwarding node.
"""

import re
import urllib2
import cStringIO

from gip_common import cp_get, cp_getList, getLogger, printTemplate
from gip_cluster import getClusterName
from condor_common import condorCommand
from gip_ldap import read_bdii, read_ldap
from gip_sections import forwarding
import gip_sets as sets

log = getLogger("GIP.Forwarding")

split_re = re.compile("\s*,?\s*")

class Forwarding(BatchSystem):

    def __init__(self, cp):
        super(Forwarding, self).__init__(self)
        self.getInputs()
        self.sites = self.cp_getList(cp, forwarding, "sites", [])
        if not self.sites:
            exc = Exception("No forwarding sites given!")
            log.exception(exc)
            raise exc
        self.determineGlue()

    def getInputs(self):
        """
        Read in the various inputs to the forwarding node.
        """
        self.inputs = self.cp_getList(cp, forwarding, "input", [])
        results = []
        orig_bdii = self.cp.get("bdii", "endpoint")
        for input in inputs:
            if input.startswith('bdii'):
                self.cp.set("bdii", "endpoint", input)
                try:
                results += read_bdii(cp, multi=True)
            else:
                try:
                    fp = urllib2.urlopen(input)
                    results += read_ldap(fp, multi=True)
                except:
                    log.warning("Unable to read: %s" % input)
        if not results:
            exc = Exception("No valid inputs were read!")
            log.exception(exc)
            raise exc
        self.data = results

    def filterObject(self, object):
        results = []
        for entry in self.data:
            if object in entry.objectClass:
                results.append(entry)
        return results

    def determineGlue(self):
        """
        Determine what GlueCE and GlueVOInfo we want to forward out of the
        input we have recieved.
        """
        unique_ids = []
        for site in self.filterObject('GlueSite'):
            if site.glue['SiteName'][0] in self.sites:
                unique_ids.append(site.glue['SiteUniqueID'][0])
        cluster_unique_ids = []
        for cluster in self.filterObject("GlueCluster"):
            for site_id in unique_ids:
                key = 'GlueSiteUniqueID=%s' % site_id
                if key in cluster.glue['ForeignKey']:
                    cluster_unique_ids.append( \
                        cluster.glue['ClusterUniqueID'][0])
        self.forward_ces = []
        ce_unique_ids = []
        for ce in self.filterObject('GlueCE'):
            for cluster_id in cluster_unique_ids:
                key = 'GlueClusterUniqueID=%s' % cluster_id
                if key in ce.glue['ForeignKey']:
                    self.forward_ces.append(ce)
        self.forward_voviews = []
        for voview in self.filterObject("GlueVOView"):
            for id in ce_unique_ids:
                key = 'GlueCEUniqueID=%s' % id
                if key in voview.glue['ChunkKey']:
                    self.forward_voviews.append(voview)

    def groupAttribute(self, attr, listing, type='int'):
        results = {}
        for ce in listing:
            try:
                host = ce.glue['CEHostingCluster']
                if type == 'int':
                    val = [0]
                else:
                    val = [] 
                if attr in ce.glue:
                    val = ce.glue[attr]
                if attr in ce.nonglue:
                    val = ce.nonglue[attr]
                if type == 'int':
                    val = sum([int(i) for i in val])
                if host in results and results[host] and not val:
                    continue
                results[host] = val 
            except:
                pass

    def addCEAttribute(self, attr):
        values = groupAttribute(attr, self.forward_ces).values()
        return sum(values)

    def unionCEAttribute(self, attr):
        values = groupAttribute(attr, self.forward_ces).values()
        results = sets.Set()
        for val in values:
            results.update(val)
        return list(results)

    def getLrmsInfo(self):
        """
        Returns the condor version used by the forwarding node.

        @returns: The condor version
        @rtype: string
        """
        for line in condorCommand(condor_version, cp):
            if line.startswith("$CondorVersion:"):
                version = line[15:].strip()
                log.info("Running condor version %s." % version)
                return version
        ve = ValueError("Bad output from condor_version.")
        log.exception(ve)
        raise ve

    def printAdditional(self):
        """
        Print out the subclusters.
        """
        template = '%s'
        chunk_key = ['GlueClusterUniqueID=%s' % getClusterName(self.cp)]
        for entry in self.data:
            if 'GlueSubCluster' not in entry.objectClass:
                continue
            entry.glue['ChunkKey'] = chunk_key
            printTemplate(template, entry.to_ldif()

    def getQueueList(self):
        return ['default']

    def getVoQueues(self):
        acbrs = self.unionCEAttribute('CEAccessControlBaseRule')
        fqans = [normalizeFQAN(i) for i in acbrs]
        vos = [i.split('/')[1] for i in fqans]
        queues = self.getQueueList()
        results = []
        for vo vos:
            for queue in queues:
                results.append((vo, queue))
        return results

