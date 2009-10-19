
"""
Module for interacting with SGE.
"""

import gip_sets as sets

from gip_common import  getLogger, VoMapper, voList, parseRvf
from xml_common import parseXmlSax
from gip.batch_systems.sge_sax_handler import QueueInfoParser, JobInfoParser, \
    sgeCommand, convert_time_to_secs
from gip.batch_systems.batch_system import BatchSystem

log = getLogger("GIP.SGE")

sge_version_cmd = "qstat -help"
sge_queue_info_cmd = 'qstat -f -xml'
sge_queue_config_cmd = 'qconf -sq %s'
sge_job_info_cmd = 'qstat -xml -u \*'
sge_queue_list_cmd = 'qconf -sql'

class SgeBatchSystem(BatchSystem):

    def __init__(self):
        self._version = None

    def getLrmsInfo(self):
        if self._version != None:
            return self._version
        for line in runCommand(sge_version_cmd):
            self._version = "sge", line.strip('\n')
            return self._version
    raise Exception("Unable to determine LRMS version info.")

    def parseNodes(cp):
        """
        Parse the node information from SGE.  Using the output from qhost, 
        determine:
    
            - The number of total CPUs in the system.
            - The number of free CPUs in the system.
            - A dictionary mapping PBS queue names to a tuple containing the
                (totalCPUs, freeCPUs).
        """
        raise NotImplementedError()

    def getQueueList(self):
        """
        Returns a list of all the queue names that are supported.

        @param cp: Site configuration
        @returns: List of strings containing the queue names.
        """
        vo_queues = self.getVoQueues()
        queues = sets.Set()
        for vo, queue in vo_queues:
            queues.add(queue)
        return queues

    def getVoQueues(self, cp):
        voMap = self.vo_map
        try:
            queue_exclude = [i.strip() for i in self.cp.get("sge",
                "queue_exclude").split(',')]
        except:
            queue_exclude = []

        # SGE has a special "waiting" queue -- ignore it.
        queue_exclude.append('waiting')

        vo_queues = []
        queue_list, q = self.getQueueInfo()
        rvf_info = parseRvf('sge.rvf')
        rvf_queue_list = rvf_info.get('queue', {}).get('Values', None)
        if rvf_queue_list:
            rvf_queue_list = rvf_queue_list.split()
            log.info("The RVF lists the following queues: %s." % ', '.join( \
                rvf_queue_list))
        else:
            log.warning("Unable to load a RVF file for SGE.")
        for queue, qinfo in queue_list.items():
            if rvf_queue_list and queue not in rvf_queue_list:
                continue
            if queue in queue_exclude:
                continue
            volist = sets.Set(voList(cp, voMap))
            try:
                whitelist = [i.strip() for i in self.cp.get("sge",
                    "%s_whitelist" % queue).split(',')]
            except:
                whitelist = []
            whitelist = sets.Set(whitelist)
            try:
                blacklist = [i.strip() for i in self.cp.get("sge",
                    "%s_blacklist" % queue).split(',')]
            except:
                blacklist = []
            blacklist = sets.Set(blacklist)
            #if 'user_list' in qinfo:
            #    acl_vos = self.parseAclInfo(queue, qinfo)
            #    if acl_vos:
            #        volist.intersection_update(acl_vos)
            for vo in volist:
                if (vo in blacklist or "*" in blacklist) and \
                        ((len(whitelist) == 0) or vo not in whitelist):
                    continue
                vo_queues.append((vo, queue))
        return vo_queues

