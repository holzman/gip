#!/usr/bin/env python

import os
import sys
import unittest

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))

from gip_testing import runTest, streamHandler
from gip_common import config
from gip_ldap import read_ldap
from gip.batch_systems.pbs import PbsBatchSystem

class TestCluster(unittest.TestCase):

    def setUpLDAP(self, filename=None, provider="cluster"):
        if filename != None:
             self.filename = filename
        os.environ['GIP_TESTING'] = "1"
        self.cp = config(self.filename)
        cmd = os.path.expandvars("$GIP_LOCATION/providers/" \
            "%%s.py --config %s" % self.filename)
        pbs = PbsBatchSystem(self.cp)
        self.queue_list = pbs.getQueueList()
        if provider == "cluster":
            cmd = cmd % "site_topology"
        elif provider == "batch":
            cmd = cmd % "batch_system"
        else:
            raise Exception("Unknown provider")
        print >> sys.stderr, "Used command", cmd
        fd = os.popen(cmd)
        self.entries = read_ldap(fd, multi=True)
        self.exit_status = fd.close()

    def filter_types(self, kind, no_results_is_error=True):
        results = [i for i in self.entries if kind in i.objectClass]
        self.failUnless(results, msg="No %s returned by provider" % kind)
        return results

    def verify_other_ces(self):
        """
        Verify the other_ces setting
        """
        clusters = self.filter_types("GlueCluster")
        queue_names = self.queue_list
        queue_names = ['jobmanager-pbs-%s' % i for i in queue_names]
        all_ces = ['red.unl.edu', 'gpn-husker.unl.edu']
        for cluster in clusters:
            all_combos = []
            for queue in queue_names:
                for ce in all_ces:
                    all_combos.append((ce, queue))
            for fk in cluster.glue['ForeignKey']:
                if not fk.startswith('GlueCEUniqueID'):
                    continue
                ce_uniq = fk.split('=')[-1]
                ce = ce_uniq.split(':')[0].split('/')[0]
                queue = ce_uniq.split('/')[-1]
                self.failUnless(queue in queue_names, msg="Unknown queue %s" % \
                    queue)
                self.failUnless(ce in all_ces, msg="Unknown CE %s" % ce)
                self.failUnless((ce, queue) in all_combos, msg="Unexpected " \
                    "combination: %s, %s" % (ce, queue))
                all_combos.remove((ce, queue))
            self.failIf(all_combos, msg="Expected additional CE FKs: %s" % \
                ", ".join(all_combos))

    def verify_cluster_name(self):
        """
        Verify the non-standard cluster name
        """
        clusters = self.filter_types("GlueCluster")
        for cluster in clusters:
            cname = cluster.glue['ClusterName'][0]
            self.failUnless(cname == 'red.unl.edu',
                msg="Unexpected cluster name: %s" % cname)

    def verify_hosting_cluster(self):
        """
        Verify the non-standard cluster name changes the hosting cluster in
        the GlueCE LDIF.
        """
        ces = self.filter_types("GlueCE")
        for ce in ces:
            cname = ce.glue['CEHostingCluster'][0]
            self.failUnless(cname == 'red.unl.edu',
                msg="Unexpected hosting cluster: %s" % cname)
            fk = ce.glue['ForeignKey'][0]
            self.failUnless(fk == 'GlueClusterUniqueID=red.unl.edu',
                msg="Incorrect cluster foreign key for GlueCE")

    def test_topology(self):
        """
        Tests the use of a non-default cluster name in config.ini and the
        other_ces setting
        """
        self.setUpLDAP('test_configs/gpn-husker.conf', provider='cluster')
        self.verify_cluster_name()
        self.verify_other_ces()

    def test_ce(self):
        """
        Tests the GlueCEHostingCluster changes with the config.ini entry
        """
        self.setUpLDAP("test_configs/gpn-husker.conf", provider="batch")
        self.verify_hosting_cluster()

def main():
    cp = config("test_configs/gpn-husker.conf")
    stream = streamHandler(cp)
    runTest(cp, TestCluster, stream, per_site=False)

if __name__ == '__main__':
    main()

