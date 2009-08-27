from xml.sax.handler import ContentHandler

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
        import copy
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
        import copy
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

