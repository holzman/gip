
from gip_sets import Set
from gip_common import VoMapper

class BatchSystem(object):

    """
    Super class for batch system implementations.
    """

    def __init__(self, cp, vo_map=None):
        self.cp = cp
        if not vo_map:
            self.vo_map = VoMapper(cp)

    def getLrmsInfo(self):
        """
        Return the version string from the batch system.

        @returns: The condor version
        @rtype: string
        """
        raise NotImplementedError()

    def getJobsInfo(self):
        """
    Return information about the jobs currently running in the batch system.
    
    The return value is a dictionary of dictionaries; the keys for the
    top-level dictionary are queue names; the values are queuedata dictionaries
    
    The queuedata dicts have key:val pairs of voname: voinfo, where voinfo is
    a dictionary with the following keys:
       - running: Number of VO running jobs in this queue.
       - wait: Number of VO waiting jobs in this queue.
       - total: Number of VO total jobs in this queue.
    
    @param vo_map: A VoMapper object which is used to map user names to VOs.
    @param cp: Site configuration object
    @return: A dictionary containing queue job information.
        """
        raise NotImplementedError()

    def getVoQueues(self):
        """
    Determine the (vo, queue) tuples for this LRMS.  This allows for central
    configuration of which VOs are advertised.

    Sites will be able to blacklist queues they don't want to advertise,
    whitelist certain VOs for a particular queue, and blacklist VOs from queues.

    @param cp: Site configuration
    @returns: A list of (vo, queue) tuples representing the queues each VO
        is allowed to run in.
        """
        raise NotImplementedError()

    def getQueueList(self):
        """
    Returns a list of all the queue names that are supported.

    @param cp: Site configuration
    @returns: List of strings containing the queue names.
        """
        vo_queues = self.getVoQueues()
        queues = Set()
        for vo, queue in vo_queues:
            queues.add(queue)
        return list(queues)

    def getQueueInfo(self):
        """
    Looks up the queue information from the batch system.

    The returned dictionary contains the following keys:
    
      - B{status}: Production, Queueing, Draining, Closed
      - B{priority}: The priority of the queue.
      - B{max_wall}: Maximum wall time.
      - B{max_running}: Maximum number of running jobs.
      - B{running}: Number of running jobs in this queue.
      - B{wait}: Waiting jobs in this queue.
      - B{total}: Total number of jobs in this queue.

    @param cp: Configuration of site.
    @returns: A dictionary of queue data.  The keys are the queue names, and
        the value is the queue data dictionary.
        """
        raise NotImplementedError()

    def parseNodes(self):
        """
    Parse the node information from PBS.  Using the output from pbsnodes, 
    determine:
    
        - The number of total CPUs in the system.
        - The number of free CPUs in the system.
        - A dictionary mapping PBS queue names to a tuple containing the
            (totalCPUs, freeCPUs).
        """
        raise NotImplementedError()

    def printAdditional(self):
        """
        This is the chance for the batch system to print out anything
        non-standard to the GLUE stream.
        """

