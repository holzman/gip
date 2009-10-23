
"""
Module for interacting with SGE.
"""

import re
from UserDict import UserDict

import gip_sets as sets

from gip_common import  getLogger, VoMapper, voList, parseRvf
from xml_common import parseXmlSax
from gip.batch_systems.sge_sax_handler import QueueInfoParser, JobInfoParser, \
    sgeCommand, convert_time_to_secs, runCommand, HostInfoParser
from gip.batch_systems.batch_system import BatchSystem

log = getLogger("GIP.SGE")

sge_version_cmd = "qstat -help"
sge_queue_info_cmd = 'qstat -f -xml'
sge_queue_config_cmd = 'qconf -sq %s'
sge_job_info_cmd = 'qstat -xml -u \*'
sge_queue_list_cmd = 'qconf -sql'
sge_qhost_cmd = 'qhost -xml'

class SGEQueueConfig(UserDict):
    def __init__(self, config_fp):
        from gip_common import _Constants
        UserDict.__init__(self, dict=None)
        self.constants = _Constants()
        self.digest(config_fp)

    def digest(self, config_fp):
        for pair in config_fp:
            if len(pair) > 1:
                key_val = pair.split()
                if len(key_val) > 1:
                    self[key_val[0].strip()] = key_val[1].strip()


class SgeBatchSystem(BatchSystem):

    def __init__(self, cp):
        super(SgeBatchSystem, self).__init__(cp)
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
        xml = runCommand(sge_qhost_cmd)
        handler = HostInfoParser()
        parseXmlSax(xml, handler)
        hosts = handler.getHosts()
        total = 0
        for host, data in hosts.items():
            if host == 'global':
                continue
            try:
                total += int(data['num_proc'])
            except:
                pass

        xml = runCommand(sge_job_info_cmd)
        handler = JobInfoParser()
        parseXmlSax(xml, handler)
        job_info = handler.getJobInfo()
        free = total
        for job in job_info:
            try:
                state = job['state']
                slots = int(job['slots'])
                if state == 'r':
                    free -= slots
            except:
                pass
        free = max(free, 0)
        log.info("There were %i cores total, %i free." % (total, free))
        return total, free, {}

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

    def getVoQueues(self):
        voMap = self.vo_map
        try:
            queue_exclude = [i.strip() for i in self.cp.get("sge",
                "queue_exclude").split(',')]
        except:
            queue_exclude = []

        # SGE has a special "waiting" queue -- ignore it.
        queue_exclude.append('waiting')

        vo_queues = []
        queue_list = self.getQueueInfo()
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
            volist = sets.Set(voList(self.cp, voMap))
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

    def getJobsInfo(self):
        xml = runCommand(sge_job_info_cmd)
        handler = JobInfoParser()
        parseXmlSax(xml, handler)
        job_info = handler.getJobInfo()
        queue_jobs = {}

        for job in job_info:
            user = job['JB_owner']
            state = job['state']
            queue = job.get('queue_name', '')
            if queue.strip() == '':
                queue = 'waiting'
            queue = queue.split('@')[0]
            try:
                vo = self.vo_map[user].lower()
            except:
                # Most likely, this means that the user is local and not
                # associated with a VO, so we skip the job.
                continue

            voinfo = queue_jobs.setdefault(queue, {})
            info = voinfo.setdefault(vo, {"running":0, "wait":0, "total":0})
            if state == "r":
                info["running"] += 1
            else:
                info["wait"] += 1
            info["total"] += 1
            info["vo"] = vo
        log.debug("SGE job info: %s" % str(queue_jobs))
        return queue_jobs

    def getQueueInfo(self):
        """
        Looks up the queue and job information from SGE.

        @param cp: Configuration of site.
        @returns: A dictionary of queue data and a dictionary of job data.
        """
        queue_list = {}
        xml = runCommand(sge_queue_info_cmd)
        handler = QueueInfoParser()
        parseXmlSax(xml, handler)
        queue_info = handler.getQueueInfo()
        for queue, qinfo in queue_info.items():

            if queue == 'waiting':
                continue

            # get queue name
            name = queue.split("@")[0]
            if name not in queue_list:
                queue_list[name] = {'slots_used': 0, 'slots_total': 0,
                'slots_free': 0, 'wait' : 0, 'name' : name}
            q = queue_list[name]
            #log.info("Queue name %s; info %s" % (name, str(qinfo)))
            try:
                q['slots_used'] += int(qinfo['slots_used'])
            except:
                pass
            try:
                q['slots_total'] += int(qinfo['slots_total'])
            except:
                pass
            q['slots_free'] = q['slots_total'] - q['slots_used']
            if 'arch' in qinfo:
                q['arch'] = qinfo['arch']
            q['max_running'] = q['slots_total']
            q['running'] = q['slots_used']
            q['total'] = 0

            try:
                state = queue_info[queue]["state"]
                if state.find("d") >= 0 or state.find("D") >= 0:
                    status = "Draining"
                elif state.find("s") >= 0:
                    status = "Closed"
                else:
                    status = "Production"
            except:
                status = "Production"

            q['status'] = status
            q['priority'] = 0  # No such thing that I can find for a queue

            # How do you handle queues with no limit?
            sqc = SGEQueueConfig(sgeCommand(sge_queue_config_cmd % name,
                self.cp))

            try:
                q['priority'] = int(sqc['priority'])
            except:
                pass

            max_wall_hard = convert_time_to_secs(sqc.get('h_rt', 'INFINITY'))
            max_wall_soft = convert_time_to_secs(sqc.get('s_rt', 'INFINITY'))
            max_wall = min(max_wall_hard, max_wall_soft)
            try:
                q['max_slots'] = int(sqc['slots'])
            except:
                q['max_slots'] = 1

            try:
                q['max_wall'] = min(max_wall, q['max_wall'])
            except:
                q['max_wall'] = max_wall

            user_list = sqc.get('user_lists', 'NONE')
            if user_list.lower().find('none') >= 0:
                user_list = re.split('\s*,?\s*', user_list)
            if 'all' in user_list:
                user_list = []
            q['user_list'] = user_list

            queue_list[name] = q

        waiting_jobs = 0
        for job in queue_info['waiting']:
            waiting_jobs += 1
        queue_list['waiting'] = {'waiting': waiting_jobs}

        return queue_list #, queue_info

