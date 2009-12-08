
"""
Module for interacting with SGE.
"""

import re
import os
from UserDict import UserDict

import gip_sets as sets

from gip_common import voList, parseRvf, cp_get
from gip_logging import getLogger
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
        self._qconf_cache = {}
        self._nodes_cache = None
        self._voqueues_cache = None
        self._sge_job_cache = None
        self._jobs_cache = None
        self._queue_cache = None
        self._pending_cache = {}

    def getLrmsInfo(self):
        if self._version != None:
            return self._version
        for line in runCommand(sge_version_cmd):
            self._version = "sge", line.strip('\n')
            return self._version
        raise Exception("Unable to determine LRMS version info.")

    def parseNodes(self):
        """
        Parse the node information from SGE.  Using the output from qhost, 
        determine:
    
            - The number of total CPUs in the system.
            - The number of free CPUs in the system.
            - A dictionary mapping PBS queue names to a tuple containing the
                (totalCPUs, freeCPUs).
        """
        if self._nodes_cache != None:
            return self._nodes_cache
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

        job_info = self._getJobsInfo()
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
        self._nodes_cache = total, free, {}
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
        if self._voqueues_cache != None:
            return self._voqueues_cache
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
        self._voqueues_cache = vo_queues
        return vo_queues

    def getJobsInfo(self):
        if self._jobs_cache != None:
            return self._jobs_cache
        job_info = self._getJobsInfo()
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

        pending_jobs = self._getPendingInfo(queue_jobs.keys())
        for queue, data in pending_jobs.items():
            voinfo = queue_jobs.setdefault(queue, {})
            for user, pending in data.items():
                try:
                    vo = self.vo_map[user].lower()
                except:
                    continue
                info = voinfo.setdefault(vo, {"running":0, "wait":0, "total":0})
                info['wait'] += pending

        log.debug("SGE job info: %s" % str(queue_jobs))
        self._jobs_cache = queue_jobs
        return queue_jobs

    def getQueueInfo(self):
        """
        Looks up the queue and job information from SGE.

        @param cp: Configuration of site.
        @returns: A dictionary of queue data and a dictionary of job data.
        """
        if self._queue_cache != None:
            return self._queue_cache
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
            if name not in self._qconf_cache:
                sqc = SGEQueueConfig(sgeCommand(sge_queue_config_cmd % name,
                    self.cp))
                self._qconf_cache[name] = sqc
            sqc = self._qconf_cache[name]

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

        pending_info = self._getPendingInfo(queue_list.keys())
        for queue, data in pending_info.items():
            queue_list[queue]['wait'] += sum(data.values())

        self._queue_cache = queue_list
        return queue_list #, queue_info

    def _maxQueue(self, queue_jobs):
        """
        Given a dictionary of queues -> # of jobs, determine which queue has
        the most active queue.
        This will throw an exception if the queue_jobs parameter is empty
        """
        most_active_queue = queue_jobs.keys()[0]
        max_queue_size = queue_jobs[most_active_queue]
        for qname, slots in queue_jobs.items():
            if slots > max_queue_size:
                most_active_queue = qname
                max_queue_size = slots
        return most_active_queue

    def _getJobsInfo(self):
        if self._sge_job_cache != None:
            return self._sge_job_cache
        xml = runCommand(sge_job_info_cmd)
        handler = JobInfoParser()
        parseXmlSax(xml, handler)
        self._sge_job_cache = handler.getJobInfo()
        return self._sge_job_cache

    def _getPendingInfo(self, queue_list):
        """
        SGE doesn't let us know what queue the jobs belong to until the job
        execution starts.  Hence, we look at what queue the user has the
        most jobs in and arbitrarily assign the pending jobs there.

        This method takes in a list of valid queue names and returns a dict;
        the dictionary maps a queue_name to a dictionary mapping usernames to
        number of pending jobs
        """
        queue_set = sets.ImmutableSet(queue_list)
        if queue_set in self._pending_cache:
            return self._pending_cache[queue_set]
        job_info = self._getJobsInfo()
        pending_jobs = {}
        running_jobs = {}
        queue_jobs = {}
        for job in job_info:
            user = job['JB_owner']
            state = job['state']
            try:
                slots = int(job['slots'])
            except:
                slots = 1
            queue = job.get('queue_name', '')
            if queue.strip() == '':
                queue = 'waiting'
            queue = queue.split('@')[0]
            if state == "qw":
                jobs = pending_jobs.setdefault(user, 0)
                pending_jobs[user] = jobs + slots
            else:
                user_jobs = running_jobs.setdefault(user, {})
                jobs = user_jobs.setdefault(queue, 0)
                user_jobs[queue] = jobs + slots
                jobs = queue_jobs.setdefault(queue, 0)
                queue_jobs[queue] = jobs + slots

        if not queue_jobs:
            if not queue_list:
                log.warning("No queues configured, but pending jobs; " \
                    "will return nothing.")
                self._queue_cache = {}
                return self._queue_cache
            most_active_queue = queue_list[0]
            log.warning("No active queues but there are pending jobs; will" \
                " guess the jobs belong to an arbitrary queue, %s." % \
                most_active_queue)
        else:
            most_active_queue = self._maxQueue(queue_jobs)
            if most_active_queue not in queue_list:
                if not queue_list:
                    log.warning("No queues configured, but pending jobs; " \
                        "will return nothing.")
                    self._queue_cache = {}
                    return self._queue_cache
                tmp_queue = queue_list[0]
                log.info("Most active queue %s is not already in queue list; " \
                    "this may indicate an error.  Arbitrarily picking %s as " \
                    "the most active queue." % (most_active_queue, tmp_queue))
                most_active_queue = tmp_queue
        log.info("Most active queue is %s; any user with only pending jobs " \
            "will be assigned to this one." % most_active_queue)

        results = {}
        for user, pending in pending_jobs.items():
            if user not in running_jobs or not running_jobs[user]:
                log.info("User %s will have their %i pending jobs assigned to "\
                    "queue %s because they are not running in any other " \
                    "queue." % (user, pending, most_active_queue))
                user_active_queue = most_active_queue
            else:
                user_active_queue = self._maxQueue(running_jobs[user])
                if len(running_jobs[user]) == 1:
                    log.info("User %s will have their %i pending jobs " \
                        "assigned to queue %s because they already have %i " \
                        "jobs running there." % (user, pending,
                        user_active_queue,
                        running_jobs[user][user_active_queue]))
                else:
                    log.info("User %s will have their %i pending jobs " \
                        "assigned to queue %s because they already have %i " \
                        "jobs running there, more than any other queue." % \
                        (user, pending, user_active_queue,
                        running_jobs[user][user_active_queue]))
            if user_active_queue in queue_list:
                tmp = results.setdefault(user_active_queue, {})
                tmp.setdefault(user, 0)
                tmp[user] += pending
            else:
                log.warning("Most active user queue is %s, but it wasn't found"\
                    " in the queue list; this is probably an internal error." \
                    "  Assigning user %s's %i pending jobs to most active " \
                    "overall queue, %s" % (user_active_queue, user, pending,
                    most_active_queue))
                tmp = results.setdefault(most_active_queue, {}) 
                tmp.setdefault(user, 0) 
                tmp[user] += pending
        self._pending_cache[queue_set] = results
        return results

    def bootstrap(self):
        """
        If it exists, source
        $SGE_ROOT/$SGE_CELL/common/settings.sh
        """
        sge_root = cp_get(self.cp, "sge", "sge_root", "")
        if not sge_root:
            log.warning("Could not locate sge_root in config file!  Not " \
                "bootstrapping SGE environment.")
            return
        sge_cell = cp_get(self.cp, "sge", "sge_cell", "")
        if not sge_cell:
            log.warning("Could not locate sge_cell in config file!  Not " \
                "bootstrapping SGE environment.")
            return
        settings = os.path.join(sge_root, sge_cell, "common/settings.sh")
        if not os.path.exists(settings):
            log.warning("Could not find the SGE settings file; looked in %s" % \
                settings)
            return
        cmd = "/bin/sh -c 'source %s; /usr/bin/env'" % settings
        fd = os.popen(cmd)
        results = fd.read()
        if fd.close():
            log.warning("Unable to source the SGE settings file; tried %s." % \
                settings)
        for line in results.splitlines():
            line = line.strip()
            info = line.split('=', 2)
            if len(info) != 2:
                continue
            os.environ[info[0]] = info[1]

