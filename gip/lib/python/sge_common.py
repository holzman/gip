# Tony's notes
#
# CE must have SGE_ROOT mounted
#
# get list of queues: qconf -sql
# get queue properties: qconf -sq queue_name
# Master hostname is in <sge_root>/<cell>/common/act_qmaster
# get list of execution hosts: qconf -sel
# get list of requestable attributes: qconf -scl
# get list of ACL's: qconf -sul
#

"""
Module for interacting with SGE.
"""

import re

from gip_logging import getLogger
from gip_common import  VoMapper, voList, parseRvf
from xml_common import parseXmlSax
from gip.batch_systems.sge_sax_handler import QueueInfoParser, JobInfoParser, \
    sgeCommand
from gip_testing import runCommand
from UserDict import UserDict
import gip_sets as sets

log = getLogger("GIP.SGE")

sge_version_cmd = "qstat -help"
sge_queue_info_cmd = 'qstat -f -xml'
sge_queue_config_cmd = 'qconf -sq %s'
sge_job_info_cmd = 'qstat -xml -u \*'
sge_queue_list_cmd = 'qconf -sql'
sge_host_cmd = 'qhost -xml'

# h_rt - hard real time limit (max_walltime)

def getLrmsInfo(cp):
    for line in runCommand(sge_version_cmd):
        return line.strip('\n')
    raise Exception("Unable to determine LRMS version info.")

def convert_time_to_secs(entry, infinity=9999999, error=None):
    """
    Convert the output of a time-related field in SGE to seconds.

    This handles the HH:MM:SS format plus the text "infinity"
    """
    if error == None:
        error = infinity
    entry = entry.split(':')
    if len(entry) == 3:
        try:
            hours, mins, secs = int(entry[0]), int(entry[1]), int(entry[2])
        except:
            log.warning("Invalid time entry: %s" % entry)
            return error
        return hours*3600 + mins*60 + secs
    elif len(entry) == 1:
        entry = entry[0]
        if entry.lower().find('inf') >= 0:
            return infinity
        else:
            try:
                return int(entry)
            except:
                return infinity
    else:
        return error

def getQueueInfo(cp):
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
        q = queue_list.get(name, {'slots_used': 0, 'slots_total': 0,
            'slots_free': 0, 'waiting' : 0, 'name' : name})
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
        sqc = SGEQueueConfig(sgeCommand(sge_queue_config_cmd % name, cp))

        try:
            q['priority'] = int(sqc['priority'])
        except:
            pass

        max_wall_hard = convert_time_to_secs(sqc.get('h_rt', 'INFINITY'))
        max_wall_soft = convert_time_to_secs(sqc.get('s_rt', 'INFINITY'))
        max_wall = min(max_wall_hard, max_wall_soft)

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

    return queue_list, queue_info

def getJobsInfo(vo_map, cp):
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
            vo = vo_map[user].lower()
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

def getQueueList(cp):
    """
    Returns a list of all the queue names that are supported.

    @param cp: Site configuration
    @returns: List of strings containing the queue names.
    """
    vo_queues = getVoQueues(cp)
    queues = sets.Set()
    for vo, queue in vo_queues:
        queues.add(queue)
    return queues

def getVoQueues(cp):
    voMap = VoMapper(cp)
    try:
        queue_exclude = [i.strip() for i in cp.get("sge",
            "queue_exclude").split(',')]
    except:
        queue_exclude = []

    # SGE has a special "waiting" queue -- ignore it.
    queue_exclude.append('waiting')
   
    log.info("Excluded queues for SGE: %s" % ", ".join(queue_exclude))
 
    vo_queues = []
    queue_list, q = getQueueInfo(cp)
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
            whitelist = [i.strip() for i in cp.get("sge",
                "%s_whitelist" % queue).split(',')]
        except:
            whitelist = []
        whitelist = sets.Set(whitelist)
        try:
            blacklist = [i.strip() for i in cp.get("sge",
                "%s_blacklist" % queue).split(',')]
        except:
            blacklist = []
        blacklist = sets.Set(blacklist)
        if 'user_list' in qinfo:
            acl_vos = parseAclInfo(queue, qinfo, voMap)
            if acl_vos:
                volist.intersection_update(acl_vos)
        if whitelist:
            log.info("Queue %s; whitelist %s" % (queue, ", ".join(whitelist)))
        if blacklist:
            log.info("Queue %s; blacklist %s" % (queue, ", ".join(blacklist)))
        for vo in volist:
            if (vo in blacklist or "*" in blacklist) and ((len(whitelist) == 0)\
                or vo not in whitelist):
                    continue
            vo_queues.append((vo, queue))
    return vo_queues

def parseAclInfo(queue, qinfo, vo_mapper):
    """
    Take a queue information dictionary and determine which VOs are in the ACL
    list.  The used keys are:

       - users: A set of all user names allowed to access this queue.
       - groups: A set of all group names allowed to access this queue.

    @param queue: Queue name (for logging purposes).
    @param qinfo: Queue info dictionary
    @param vo_mapper: VO mapper object
    @returns: A set of allowed VOs
    """
    # TODO: find a sample SGE site which uses this!
    return []
    user_list = qinfo.get('user_list', sets.Set())
    users = sets.Set()
    all_groups = grp.getgrall()
    all_users = pwd.getpwall()
    group_dict = {}
    group_list = [i[1:] for i in user_list if i.startswith('@')]
    user_list = [i for i in user_list if not i.startswith('@')]
    for group in all_groups:
        if group[0] in group_list or group[2] in group_list:
            users.add(group[0])
        group_dict[group[2]] = group[0]
    for user in all_users:
        try:
            group = group_dict[user[3]]
        except:
            continue
        if group[0] in group_list or user[3] in group_list:
            users.add(group[0])
    vos = sets.Set()
    for user in users:
        try:
            vos.add(vo_mapper[user])
        except:
            pass
    log.info("The acl info for queue %s (users %s, groups %s) mapped to %s." % \
        (queue, ', '.join(user_list), ', '.join(group_list), ', '.join(vos)))
    return vos

if __name__ == '__main__':

    qconf_output = """
name    any
type    ACL
fshare  0
oticket 0
entries @Mackenzie,@ahouston,@allen,@anderson,@bahar,@batelaan,@belashchenko, \
        @berkowitz,@bobaru,@brand,@bsbm,@caldwell,@calmit,@chandra,@chen, \
        @chess,@cheung,@choueiry,@cohen,@condor,@costello,@cse429,@cse477, \
        @cse496,@cse856,@cusack,@dbus,@deallab,@deogun,@diestler,@dimagno, \
        @dominguez,@du,@ducharme,@dzenis,@eckhardt,@elbaum,@feng,@g03,@gamess, \
        @gaskell,@geppert,@gitelson,@gladyshev,@goddard,@gogos,@haldaemon, \
        @harbison,@harbourne,@hep,@hibbing,@hochstein,@hoffman,@hu,@irmak, \
        @jaecks,@jaswal,@jaturner,@jiang,@jwang7,@ladunga,@lhoffman,@li,@liu, \
        @loope,@lu,@lxu,@mech950,@merillium,@morc,@moriyama,@mower,@nagiocmd, \
        @narayanan,@netdump,@nopbs,@nowak,@ntadmin,@outreach,@parker, \
        @parkhurst,@parprog,@perez,@powers,@psc_sub,@pytlikzillig,@ramamurthy, \
        @rcfsrv,@reichenbach,@reid,@riethoven,@rowe,@sabirianov,@samal, \
        @sayood,@scott,@sellmyer,@seth,@shadwick,@shea,@sicking,@snow,@soh, \
        @soulakova,@sridhar,@srisa-an,@starace,@stezowski,@subbiah,@swanson, \
        @tsymbal,@tuan,@tyre,@umstadter,@uno,@vcr,@wang,@woldt,@woodward, \
        @xwang,@yang,@zeng,@zhang
"""
    import cStringIO
    fp = cStringIO.StringIO()
    fp.write(qconf_output)
    fp.seek(0)
    for line in sgeOutputFilter(fp):
        print line.strip()

