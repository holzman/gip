
import os
import statvfs

from gip_common import cp_get, cp_getInt, getLogger
from gip_testing import runCommand
from gip.bestman.BestmanInfo import BestmanInfo

log = getLogger("GIP.Storage.Hadoop")

class HadoopInfo(BestmanInfo):

    """
    A Hadoop storage element is a HDFS system with a BeStMan endpoint.

    For the most part, this does the same thing as the BestmanInfo class, with
    a few calls added for better detection of space used/free based upon the
    current replication factor.
    """

    def __init__(self, cp, **kw):
        super(HadoopInfo, self).__init__(cp, **kw)
        self.rep_factor = 2

    def run(self):
        super(HadoopInfo, self).run()
        rep_factor = self.cp_getInt(cp, self.section, "replication_factor", 0)
        if rep_factor == 0:
            rep_factor = self.calculate_rep_factor() 
        self.rep_factor = rep_factor

    def runHadoopCommand(self, cmd):
        if 'JAVA_HOME' not in os.environ:
            os.environ['JAVA_HOME'] = '/usr/java/default'
        if 'HADOOP_CONF_DIR' not in os.environ:
            os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop'
        fd = runCommand(cmd)
        return fd


