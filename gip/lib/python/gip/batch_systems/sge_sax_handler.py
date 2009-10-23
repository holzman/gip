
import copy
from xml.sax.handler import ContentHandler

from gip_testing import runCommand

class QueueInfoParser(ContentHandler):
    def __init__(self):
        self.currentQueueInfoElmList = list(['name', 'qtype', 'slots_used',
            'slots_total', 'arch'])
        self.currentJobInfoElmList = list(['JB_job_number', 'JB_job_number',
            'JAT_prio', 'JB_name', 'JB_owner', 'state', 'JAT_start_time',
            'JB_submission_time', 'slots'])
            

    def startDocument(self):
        self.elmContents = ''
        self.QueueList = {}

    def startElement(self, name, attrs):
        self.elmContents = ''
        if name == 'Queue-List':
            self.currentQueueInfo = {}
            self.currentJobList = {}
            self.currentJobInfo = {}

        elif name == 'job_info':
            self.QueueList["waiting"] = {}
            self.currentJobList = {}
            self.inJobInfo = True

        elif name == 'queue_info':
            self.inJobInfo = False

        elif name == 'job_list':
            self.currentJobInfo = {}

        else:
            pass

    def endElement(self, name):
        if name == 'Queue-List':
            self.currentQueueInfo['jobs'] = copy.deepcopy(self.currentJobList)
            self.QueueList[self.currentQueueInfo['name']] = copy.deepcopy(\
                self.currentQueueInfo)

        elif name == 'job_list':
            self.currentJobList[self.currentJobInfo['JB_job_number']] = \
                copy.deepcopy(self.currentJobInfo)

        elif name == 'job_info':
            if self.inJobInfo:
                self.QueueList['waiting'] = copy.deepcopy(self.currentJobList)

        elif name in self.currentQueueInfoElmList:
            self.currentQueueInfo[name] = str(self.elmContents)

        elif name in self.currentJobInfoElmList:
            self.currentJobInfo[name] = str(self.elmContents)

        else:
            pass

    def characters(self, ch):
        self.elmContents += str(ch)

    def getQueueInfo(self):
        return self.QueueList


class JobInfoParser(ContentHandler):
    def __init__(self):
        self.currentJobInfoElmList = ['JB_job_number', 'JAT_prio',
            'JB_name', 'JB_owner', 'state', 'JAT_start_time',
            'JB_submission_time', 'slots', 'queue_name']

    def startDocument(self):
        self.elmContents = ''
        self.JobList = []
        self.currentJobInfo = {}

    def startElement(self, name, attrs):
        if name == 'job_list':
            self.currentJobInfo = {}
        elif name in self.currentJobInfoElmList:
            self.elmContents = ''

    def endElement(self, name):
        if name == 'job_list':
            self.JobList.append(copy.deepcopy(self.currentJobInfo))
        elif name in self.currentJobInfoElmList:
            self.currentJobInfo[str(name)] = str(self.elmContents)
            self.elmContents = ''
        else:
            pass

    def characters(self, ch):
        self.elmContents += str(ch)

    def getJobInfo(self):
        return self.JobList

class HostInfoParser(ContentHandler):

    def __init__(self):
        self.hosts = {}
        self.curHostInfo = {}
        self.curHost = None
        self.curAtt = None
        self.curVal = None

    def startElement(self, name, attrs):
        if name == 'host':
            self.curHost = attrs.get('name', 'UNKNOWN')
            self.curHostInfo = {}
        elif name == 'hostvalue':
            self.curAtt = attrs.get('name', 'UNKNOWN')
            self.curVal = ''

    def characters(self, ch):
        if self.curAtt:
            self.curVal += str(ch)

    def endElement(self, name):
        if name == 'host':
            self.hosts[self.curHost] = self.curHostInfo
        elif name == 'hostvalue':
            self.curHostInfo[self.curAtt] = self.curVal
            self.curAtt, self.curVal = None, None

    def getHosts(self):
        return self.hosts

def sgeOutputFilter(fp):
    """
    SGE's 'qconf' command has a line-continuation format which we will want to
    parse.  To accomplish this, we use this filter on the output file stream.

    You should "scrub" SGE output like this::

        fp = runCommand(<pbs command>)
        for line in pbsOutputFilter(fp):
           ... parse line ...

    Or simply,

       for line in sgeCommand(<pbs command>):
           ... parse line ...
    """
    class SGEIter:
        """
        An iterator for the SGE output.
        """

        def __init__(self, fp):
            self.fp = fp
            self.fp_iter = fp.__iter__()
            self.prevline = ''
            self.done = False

        def next(self):
            """
            Return the next full line of output for the iterator.
            """
            try:
                line = self.fp_iter.next()
                if not line.endswith('\\'):
                    result = self.prevline + line
                    self.prevline = ''
                    return result
                line = line.strip()[:-1]
                self.prevline = self.prevline + line
                return self.next()
            except StopIteration:
                if self.prevline:
                    results = self.prevline
                    self.prevline = ''
                    return results
                raise

    class SGEFilter:
        """
        An iterable object based upon the SGEIter iterator.
        """

        def __init__(self, myiter):
            self.iter = myiter

        def __iter__(self):
            return self.iter

    return SGEFilter(SGEIter(fp))

def sgeCommand(command, cp):
    """
    Run a command against the SGE batch system.
    
    Use this when talking to SGE; not only does it allow for integration into
    the GIP test framework, but it also filters and expands SGE-style line
    continuations.
    """
    fp = runCommand(command)
    return sgeOutputFilter(fp)

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

