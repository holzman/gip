
"""
Implementation of a CE for a forwarding node.
"""

import re
import urllib2
import cStringIO

from gip_common import cp_get, cp_getList, getLogger, printTemplate, \
    normalizeFQAN
from gip_cluster import getClusterName
from condor_common import condorCommand, condor_version
from gip_ldap import read_bdii, read_ldap
from gip_sections import forwarding
from batch_system import BatchSystem
import gip_sets as sets

log = getLogger("GIP.Batch.Forwarding")

split_re = re.compile("\s*,?\s*")

class Forwarding(BatchSystem):

    def __init__(self, cp):
        super(Forwarding, self).__init__(cp)
        self.getInputs()
        self.sites = cp_getList(self.cp, forwarding, "sites", [])
        if not self.sites:
            exc = Exception("No forwarding sites given!")
            log.exception(exc)
            raise exc
        self.determineGlue()

    def getInputs(self):
        """
        Read in the various inputs to the forwarding node.
        """
        self.inputs = cp_getList(self.cp, forwarding, "input", [])
        results = []
        orig_bdii = self.cp.get("bdii", "endpoint")
        for input in self.inputs:
            if input.startswith('ldap'):
                self.cp.set("bdii", "endpoint", input)
                try:
                    results += read_bdii(self.cp, multi=True)
                except Exception, e:
                    log.exception(e)
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
        self.ce_map = {}
        for ce in self.filterObject('GlueCE'):
            for cluster_id in cluster_unique_ids:
                key = 'GlueClusterUniqueID=%s' % cluster_id
                if key in ce.glue['ForeignKey']:
                    self.forward_ces.append(ce)
                    id = ce.glue['CEUniqueID'][0]
                    self.ce_map[id] = ce
                    ce_unique_ids.append(id)
        self.forward_voviews = []
        self.vo_to_ce_map = {}
        for voview in self.filterObject("GlueVOView"):
            for id in ce_unique_ids:
                key = 'GlueCEUniqueID=%s' % id
                if key in voview.glue['ChunkKey']:
                    self.forward_voviews.append(voview)
                    self.vo_to_ce_map[voview] = self.ce_map[id]

    def groupAttribute(self, attr, listing, type='int', agg_func=sum):
        results = {}
        for ce in listing:
            try:
                host = ce.glue['CEHostingCluster'][0]
                if type == 'int':
                    val = [0]
                else:
                    val = [] 
                if attr in ce.glue:
                    val = ce.glue[attr]
                if attr in ce.nonglue:
                    val = ce.nonglue[attr]
                if type == 'int':
                    val = [int(i) for i in val]
                    results_val = results.setdefault(host, 0)
                    val.append(results_val)
                    results[host] = agg_func(val)
                else:
                    results.setdefault(host, [])
                    results[host] += val
            except Exception, e:
                log.exception(e)
        return results

    def addCEAttribute(self, attr, agg_func=sum):
        values = self.groupAttribute(attr, self.forward_ces).values()
        return sum(values)

    def maxCEAttribute(self, attr):
        values = self.groupAttribute(attr, self.forward_ces).values()
        return max(values)

    def allCEAttribute(self, attr):
        result = self.groupAttribute(attr, self.forward_ces, type='str')
        return result.values()

    def groupVOAttribute(self, attr, filter_vo):
        results = {}
        for vo in self.forward_voviews:
            try:
                ce = self.vo_to_ce_map[vo]
                ce_id = ce.glue['CEUniqueID'][0]
                host = ce.glue['CEHostingCluster'][0]
                voname = self.voNameFromACBR( \
                    vo.glue['CEAccessControlBaseRule'][0])
                if filter_vo != voname:
                    continue
                val = int(vo.glue[attr][0])
                ce_dict = results.setdefault(host, {})
                if ce_id in ce_dict and ce_dict[ce_id] and not val:
                     continue
                ce_dict[ce_id] = val
            except:
                pass
        return results

    def addVOAttribute(self, attr, vo):
        values = self.groupVOAttribute(attr, vo).values()
        return sum([sum(i.values()) for i in values])

    def unionCEAttribute(self, attr):
        values = self.groupAttribute(attr, self.forward_ces, type='str').\
            values()
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
        for line in condorCommand(condor_version, self.cp):
            if line.startswith("$CondorVersion:"):
                version = line[15:].strip()
                log.info("Running condor version %s." % version)
                return "condor", version
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
            printTemplate(template, entry.to_ldif())

    def getQueueList(self):
        return ['default']

    def voNameFromACBR(self, acbr):
        return normalizeFQAN(acbr).split('/')[1]

    def getVoQueues(self):
        acbrs = self.unionCEAttribute('CEAccessControlBaseRule')
        vos = [self.voNameFromACBR(i) for i in acbrs]
        queues = self.getQueueList()
        results = []
        for vo in vos:
            for queue in queues:
                results.append((vo, queue))
        return results

    def getJobsInfo(self):
        results = {}
        for vo, _ in self.getVoQueues():
            running = self.addVOAttribute('CEStateRunningJobs', vo)
            waiting = self.addVOAttribute('CEStateWaitingJobs', vo)
            total = self.addVOAttribute('CEStateTotalJobs', vo)
            results[vo] = {'running': running, 'waiting': waiting,
                'total': total}
        return {'default': results}

    def getQueueInfo(self):
        results = {}
        states = self.unionCEAttribute('CEStateStatus')
        if 'Production' not in states:
            if 'Queueing' not in states:
                if 'Draining' not in states:
                    state = 'Closed'
                else:
                    state = 'Draining'
            else:
                state = 'Queueing'
        else:
            state = 'Production'
        results['status'] = state
        results['priority'] = 1
        results['max_wall'] = self.maxCEAttribute('CEPolicyMaxWallClockTime')
        results['max_running'] = self.maxCEAttribute('CEPolicyMaxRunningJobs')
        results['running'] = self.addCEAttribute('CEStateRunningJobs')
        results['wait'] = self.addCEAttribute('CEStateWaitingJobs')
        results['total'] = self.addCEAttribute('CEStateTotalJobs')
        return {'default': results}

    def parseNodes(self):
        total_cpus = self.specialCPUsParser(self.allCEAttribute(\
            'CEInfoTotalCPUs'))
        free_cpus = self.specialCPUsParser(self.allCEAttribute(\
            'CEStateFreeCPUs'))
        return total_cpus, free_cpus, {'default': (total_cpus, free_cpus)}

    def specialCPUsParser(self, ce_info):
        csum = 0
        for info in ce_info:
            # Skip empty list
            if not info:
                continue
            try:
                info = [int(i) for i in info]
            except:
                pass
            # If the CE has all the same values, return the value
            if min(info) == max(info):
                csum += info[0]
            else: # Return the sum of the values
                csum += sum(info)
        return csum

