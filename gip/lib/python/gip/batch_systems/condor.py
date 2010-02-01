
"""
Implementation of a Condor BatchSystem object.

This provides information about Condor's state in a generic way.
Meant to be consumed by GLUE 1.3 or GLUE 2.0 providers.
"""

import os
import sys
import re
import types

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))

import gip_sets as sets
from gip_common import cp_get, voList, cp_getBoolean, cp_getInt, addToPath
from gip_logging import getLogger

from condor_handlers import condorCommand, parseCondorXml, ClassAdParser
from batch_system import BatchSystem

log = getLogger("GIP.Condor")

condor_version = "condor_version"
condor_group = "condor_config_val -%(daemon)s GROUP_NAMES"
condor_quota = "condor_config_val -%(daemon)s GROUP_QUOTA_%(group)s"
condor_prio = "condor_config_val -%(daemon)s GROUP_PRIO_FACTOR_%(group)s"
condor_status = "condor_status -xml -constraint '%(constraint)s'"
condor_status_submitter = "condor_status -submitter -xml"
condor_job_status = "condor_q -xml -constraint '%(constraint)s'"

class CondorBatchSystem(BatchSystem):

    def __init__(self, *args, **kw):
        super(CondorBatchSystem, self).__init__(*args, **kw)
        self._nodes_cache = []
        self._results_cache = {}
        self._group_info = None
        self._jobs_info = None
        self._version_cache = None
        self._vo_queues = None
        if cp_getBoolean(self.cp, "condor", "use_collector", False):
            self._ccv_negotiator = "collector"
        else:
            self._ccv_negotiator = "negotiator"

    _version_re = re.compile("(\d+).(\d+).(\d+).*")
    def getLrmsInfo(self): #pylint: disable-msg=C0103
        """
        Get information from the LRMS (batch system).

        Returns the version of the condor client on your system.

        @returns: The condor version
        @rtype: string
        """
        if self._version_cache != None:
            return self._version_cache
        for line in condorCommand(condor_version, self.cp):
            if line.startswith("$CondorVersion:"):
                self.version = line[15:].strip()
                log.info("Running condor version %s." % self.version)
                m = self._version_re.match(self.version)
                if m:
                    self.version_major, self.version_minor = m.groups()[:2]
                else:
                    self.version_major = 0
                    self.version_minor = 0
                
                self._version_cache = "condor", self.version
                return self._version_cache
        ve = ValueError("Bad output from condor_version.")
        log.exception(ve)
        raise ve

    def getGroupInfo(self):
        """
        Get the group info from condor.

        This function wraps around getGroupInfoInternal in order to add in the
        default group for sites that have no condor groups.
        """
        groupInfo = self.getGroupInfoInternal()

        # Get the node information for condor
        try:
            total_nodes, _, _ = self.parseNodes()
        except Exception, e:
            log.exception(e)
            total_nodes = 0

        # Set up the "default" group with all the VOs which aren't already in a 
        # group
        groupInfo['default'] = {'prio': 999999, 'quota': 999999,
            'vos': sets.Set()}
        all_group_vos = sets.Set()
        total_assigned = 0
        log.debug("All group info: %s" % str(groupInfo))
        for key, val in groupInfo.items():
            if key == 'default':
                continue
            all_group_vos.update(val['vos'])
            try:
                total_assigned += val['quota']
            except:
                pass

        # Adjust the number of assigned nodes for the default group by first
        # looking at the assigned quota
        if total_nodes > total_assigned:
            log.info("There are %i assigned job slots out of %i total; " \
                "assigning the rest to the default group." % (total_assigned,
                total_nodes))
            groupInfo['default']['quota'] = total_nodes-total_assigned
        else:
            log.warning("More assigned nodes (%i) than actual nodes (%i)!" % \
                (total_assigned, total_nodes))

        # Remove any VOs who already have a queue from the default group.
        log.debug("All assigned VOs: %s" % ", ".join(all_group_vos))
        defaultVoList = voList(self.cp, vo_map=self.vo_map)
        defaultVoList = [i for i in defaultVoList if i not in all_group_vos]
        whitelist, blacklist = self.getWhiteBlackLists("default")
        if "*" in blacklist:
            defaultVoList = []
        for vo in blacklist:
            if vo in defaultVoList:
                defaultVoList.remove(vo)
        for vo in whitelist:
            if vo not in defaultVoList:
                defaultVoList.append(vo)
        groupInfo['default']['vos'] = defaultVoList
        if not groupInfo['default']['vos']:
            log.debug("No unassigned VOs; no advertising a default group")
            del groupInfo['default']
        else:
            log.info("The following VOs are assigned to the default group: %s" \
                % ", ".join(defaultVoList))

        return groupInfo

    def getGroupInfoInternal(self): #pylint: disable-msg=C0103,W0613
        """
        Get the group info from condor

        The return value is a dictionary; the key is the vo name, the values are
        another dictionary of the form {'quota': integer, 'prio': integer}

        @returns: A dictionary whose keys are condor groups and values are the 
            quota and priority of the group.
        """
        if self._group_info != None:
            return self._group_info
        fp = condorCommand(condor_group % {'daemon': self._ccv_negotiator},
            self.cp)
        output = fp.read().split(',')
        if fp.close():
            log.info("No condor groups found.")
            return {}
        retval = {}
        if (not (output[0].strip().startswith('Not defined'))) and \
                (len(output[0].strip()) > 0):
            for group in output:
                group = group.strip()
                quota = condorCommand(condor_quota, self.cp, \
                    {'group': group, "daemon": self._ccv_negotiator}).read().\
                    strip()
                prio = condorCommand(condor_prio, self.cp, \
                    {'group': group, 'daemon': self._ccv_negotiator}).read().\
                    strip()
                vos = self.guessVO(group)
                log.debug("For group %s, guessed VOs of %s" % (group,
                    ", ".join(vos)))
                curInfo = {'quota': 0, 'prio': 0, 'vos': vos}
                try:
                    curInfo['quota'] += int(quota)
                except:
                    pass
                try:
                    curInfo['prio'] += int(prio)
                except:
                    pass
                retval[group] = curInfo
        if retval:
            log.debug("The condor groups are %s." % ', '.join(retval))
        else:
            log.debug("There were no condor groups found.")
        self._group_info = retval
        return retval

    def getQueueList(self): #pylint: disable-msg=C0103
        """
        Returns a list of all the queue names that are supported.

        @returns: List of strings containing the queue names.
        log = getLogger("GIP.Condor")"""
        # Determine the group information, if there are any Condor groups
        try:
            groupInfo = self.getGroupInfo()
        except Exception, e:
            log.exception(e)
            # Default to no groups.
            groupInfo = {}

        # getGroupInfo already computes whether or not the default group should
        # exist; return the list of names directly
        return groupInfo.keys()

    def getWhiteBlackLists(self, group):
        """
        Given a group name, determine the white and black lists from the
        configuration object.
        """
        cp = self.cp
        try:
            whitelist = [i.strip() for i in cp.get("condor", "%s_whitelist" % \
                group).split(',')]
        except:
            whitelist = []
        whitelist = sets.Set(whitelist)
        try:
            blacklist = [i.strip() for i in cp.get("condor", "%s_blacklist" % \
                group).split(',')]
        except:
            blacklist = []
        blacklist = sets.Set(blacklist)
        return whitelist, blacklist

    def determineGroupVOsFromConfig(self, group):
        """
        Given a group name, determine the VOs which are
        allowed in that group; this is based solely on the config files.
        """
        cp = self.cp

        # This is the old behavior.  Base everything on (groupname)_vos
        bycp = cp_get(cp, "condor", "%s_vos" % group, None)
        if bycp:
            return [i.strip() for i in bycp.split(',')]

        # This is the new behavior.  Base everything on (groupname)_blacklist 
        # and (groupname)_whitelist.  Done to mimic the PBS configuration.
        volist = sets.Set(voList(cp, self.vo_map))
        whitelist, blacklist = self.getWhiteBlackLists(group)

        log.debug("Group %s; whitelist: %s; blacklist: %s" % (group,
            ", ".join(whitelist), ", ".join(blacklist)))

        # Return None if there's no explicit white/black list setting.
        if len(whitelist) == 0 and len(blacklist) == 0:
            return None

        # Force any VO in the whitelist to show up in the volist, even if it
        # isn't in the acl_users / acl_groups
        for vo in whitelist:
            if vo not in volist:
                volist.add(vo)
        # Apply white and black lists
        results = sets.Set()
        for vo in volist:
            if (vo in blacklist or "*" in blacklist) and ((len(whitelist) == 0)\
                    or vo not in whitelist):
                continue
            results.add(vo)
        return list(results)

    def guessVO(self, group):
        """
        From the group name, guess my VO name
        """
        bycp = self.determineGroupVOsFromConfig(group)
        vos = voList(self.cp, vo_map=self.vo_map)
        byname = sets.Set()
        for vo in vos:
            if group.find(vo) >= 0:
                byname.add(vo)
        altname = group.replace('group', '')
        altname = altname.replace('-', '')
        altname = altname.replace('_', '')
        altname = altname.strip()
        try:
            bymapper = self.vo_map[altname]
        except:
            bymapper = None
        if bycp != None:
            return bycp
        elif bymapper:
            return [bymapper]
        elif byname:
            return byname
        else:
            return []

    def _getJobsInfoInternal(self):
        """
        The "alternate" way of building the jobs info; this allows for sites to
        filter jobs based upon an arbitrary condor_q constraint.

        This is not the default as large sites can have particularly bad 
        performance for condor_q.
        """
        _results_cache = self._results_cache
        if _results_cache:
            return dict(_results_cache)
        constraint = cp_get(self.cp, "condor", "jobs_constraint", "TRUE")
        fp = condorCommand(condor_job_status, self.cp, {'constraint':
            constraint})
        handler = ClassAdParser('GlobalJobId', ['JobStatus', 'Owner',
            'AccountingGroup', 'FlockFrom']);
        fp2 = condorCommand(condor_status_submitter, self.cp)
        handler2 = ClassAdParser('Name', ['MaxJobsRunning'])
        try:
            if self.version_major <= 7 and self.version_minor < 3 and \
                    self.version_major != 0:
                for i in range(cp_getInt(self.cp, "condor", 
                        "condor_q_header_lines", 3)):
                    fp.readline()
            parseCondorXml(fp, handler)
        except Exception, e:
            log.error("Unable to parse condor output!")
            log.exception(e)
            return {}
        try:
            parseCondorXml(fp2, handler2)
        except Exception, e:
            log.error("Unable to parse condor output!")
            log.exception(e)
            return {}
        info = handler2.getClassAds()
        for item, values in handler.getClassAds().items():
            if 'AccountingGroup' in values and 'Owner' in values \
                    and values['AccountingGroup'].find('.') < 0:
                owner = '%s.%s' % (values['AccountingGroup'], values['Owner'])
            else:
                owner = values.get('AccountingGroup', values.get('Owner', None))
            if not owner:
                continue
            owner_info = info.setdefault(owner, {})
            status = values.get('JobStatus', -1)
            try:
                status = int(status)
            except:
                continue
            is_flocked = values.get('FlockFrom', False) != False
            # We ignore states Unexpanded (U, 0), Removed (R, 2), 
            # Completed (C, 4), Held (H, 5), and Submission_err (E, 6)
            if status == 1: # Idle
                owner_info.setdefault('IdleJobs', 0)
                owner_info['IdleJobs'] += 1
            elif status == 2: # Running
                if is_flocked:
                    owner_info.setdefault('FlockedJobs', 0)
                    owner_info['FlockedJobs'] += 1
                else:
                    owner_info.setdefault('RunningJobs', 0)
                    owner_info['RunningJobs'] += 1
            elif status == 5: # Held
                owner_info.setdefault('HeldJobs', 0)
                owner_info['HeldJobs'] += 1
        self._results_cache = dict(info)
        return info

    def getJobsInfo(self):
        """
        Retrieve information about the jobs in the Condor system.

        Query condor about the submitter status.  The returned job information
        is a dictionary whose keys are the VO name of the submitting user and 
        values the aggregate information about that VO's activities.  The 
        information is another dictionary showing the running, idle, held, and
        max_running jobs for that VO.

        @param vo_map: A vo_map object mapping users to VOs
        @param cp: A ConfigParser object with the GIP config information.
        @returns: A dictionary containing job information.
        """
        if self._jobs_info != None:
            return self._jobs_info
        group_jobs = {}
        queue_constraint = cp_get(self.cp, "condor", "jobs_constraint", "TRUE")
        if queue_constraint == 'TRUE':
            fp = condorCommand(condor_status_submitter, self.cp)
            handler = ClassAdParser(('Name', 'ScheddName'), ['RunningJobs',
                'IdleJobs', 'HeldJobs', 'MaxJobsRunning', 'FlockedJobs'])
            try:
                parseCondorXml(fp, handler)
            except Exception, e:
                log.error("Unable to parse condor output!")
                log.exception(e)
            results = handler.getClassAds()
        else:
            results = self._getJobsInfoInternal(self.cp)
        def addIntInfo(my_info_dict, classad_dict, my_key, classad_key):
            """
            Add some integer info contained in classad_dict[classad_key] to 
            my_info_dict[my_key]; protect against any thrown exceptions.
            If classad_dict[classad_key] cannot be converted to a number,
            default to 0.
            """
            if my_key not in my_info_dict or classad_key not in classad_dict:
                return
            try:
                new_info = int(classad_dict[classad_key])
            except:
                new_info = 0
            my_info_dict[my_key] += new_info

        all_group_info = self.getGroupInfo()

        unknown_users = sets.Set()
        for user, info in results.items():
            # Determine the VO, or skip the entry
            if isinstance(user, types.TupleType):
                user = user[0]
            name = user.split("@")[0]
            name_info = name.split('.', 1)
            if len(name_info) == 2:
                group, name = name_info
            else:
                group = 'default'
            # In case if we've been assigned a group, but it's not actually
            # configured anywhere
            if group not in all_group_info:
                group = 'default'
            log.debug("Examining jobs for group %s, user %s." % (group, name))
            try:
                vo = self.vo_map[name].lower()
            except Exception, e:
                if name in all_group_info:
                    group = name
                    if len(all_group_info[name].get('vo', [])) == 1:
                        vo = all_group_info[name]['vo']
                    else:
                        vo = 'unknown'
                else:
                    unknown_users.add(name)
                continue

            vo_jobs = group_jobs.setdefault(group, {})

            # Add the information to the current dictionary.
            my_info = vo_jobs.get(vo, {"running":0, "wait":0, "held":0, \
                'max_running':0})
            addIntInfo(my_info, info, "running", "RunningJobs")
            if cp_getBoolean(self.cp, "condor", "count_flocked", False):
                addIntInfo(my_info, info, "running", "FlockedJobs")
            addIntInfo(my_info, info, "wait", "IdleJobs")
            addIntInfo(my_info, info, "held", "HeldJobs")
            addIntInfo(my_info, info, "max_running", "MaxJobsRunning")
            my_info['total'] = my_info['running'] + my_info['wait'] + \
                my_info['held']
            vo_jobs[vo] = my_info

        if unknown_users:
            log.warning("The following users are non-grid users: %s" % \
                ", ".join(unknown_users))
        else:
            log.info("There were no unknown/non-grid users.")

        log.info("Job information: %s." % group_jobs)
        self._jobs_info = group_jobs
        return group_jobs

    def getQueueInfo(self):
        queueInfo = {}
        groupInfo = self.getGroupInfo()
        defaults = {'total': 0, 'priority': 0, 'wait': 0, 'running': 0,
            'status': 'Production'}

        # Determine the # of assigned jobs per queue
        for queue, vo_data in self.getJobsInfo().items():
            queue_data = queueInfo.setdefault(queue, dict(defaults))
            for vo, data in vo_data.items():
                queue_data['running'] += data['running']
                queue_data['wait'] += data['wait']
                queue_data['total'] += data['total']

        # Assign priority and determine # of free slots
        for queue, data in groupInfo.items():
            queue_data = queueInfo.setdefault(queue, dict(defaults))
            if 'quota' in data and data['quota'] > 0:
                queue_data['job_slots'] = data['quota']
        return queueInfo

    def getVoQueues(self):
        if self._vo_queues != None:
            return self._vo_queues
        group_info = self.getGroupInfo()
        jobs_info = self.getJobsInfo()
        results = sets.Set()
        all_vos = sets.Set(voList(self.cp, vo_map=self.vo_map))
        for group, vo_dict in jobs_info.items():
            for vo in vo_dict:
                results.add((vo, group))
        for group, vo_dict in group_info.items():
            vos = vo_dict.get('vos', sets.Set())
            for vo in vos:
                results.add((vo, group))
        log.debug("VO Queues (before adding default queue): %s", ", ".join( \
            [str(i) for i in results]))
        # The VOs in the default queue are already represented from group_info
        # No need to do this anymore.
        # Add all other VOs to default queue.
        #current_vos = sets.Set()
        #for result in results:
        #    current_vos.add(result[1])
        #all_vos.difference_update(current_vos)
        #for vo in all_vos:
        #    results.add((vo, 'default'))
        self._vo_queues = list(results)
        return self._vo_queues

    def parseNodes(self):
        """
        Parse the condor nodes.

        @param cp: ConfigParser object for the GIP
        @returns: A tuple consisting of the total, claimed, and unclaimed nodes.
        """
        if self._nodes_cache:
            return self._nodes_cache
        subtract = cp_getBoolean(self.cp, "condor", "subtract_owner", True)
        log.debug("Parsing condor nodes.")
        constraint = cp_get(self.cp, "condor", "status_constraint", "TRUE")
        fp = condorCommand(condor_status, self.cp, {'constraint': constraint})
        handler = ClassAdParser('Name', ['State'])
        parseCondorXml(fp, handler)
        total = 0
        claimed = 0
        unclaimed = 0
        for info in handler.getClassAds().values():
            total += 1
            if 'State' not in info:
                continue
            if info['State'] == 'Claimed':
                claimed += 1
            elif info['State'] == 'Unclaimed':
                unclaimed += 1
            elif subtract and info['State'] == 'Owner':
                total -= 1
        log.info("There are %i total; %i claimed and %i unclaimed." % \
                 (total, claimed, unclaimed))
        self._nodes_cache = total, claimed, {}
        return total, unclaimed, {}

    def bootstrap(self):
        try:
            condor_path = cp_get(self.cp, "condor", "condor_path", None)
            if condor_path != None:
                addToPath(condor_path)
        except Exception, e:
            log.exception(e)

