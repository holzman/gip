
"""
Common parsers and handlers for the Condor batch system.

Parses condor XML and runs condor commands
"""

import time
import types

import xml
from xml.sax import make_parser, SAXParseException
from xml.sax.handler import ContentHandler, feature_external_ges

from gip_common import getLogger
from gip_testing import runCommand

log = getLogger("GIP.Condor")

class ClassAdParser(ContentHandler):
    """
    Streaming SAX handler for the output of condor_* -xml calls; it's around
    60 times faster and has a similar reduction in required memory.

    Use this as a ContentHandler for a SAX parser; call getJobInfo afterward
    to get the information about each job.
    
    getJobInfo returns a dictionary of jobs; the key for the dictionary is the
    Condor attribute passed in as 'idx' to the constructor; the value is another
    dictionary of key-value pairs from the condor JDL, where the keys is in
    the attribute list passed to the constructor.
    """

    def __init__(self, idx, attrlist=None): #pylint: disable-msg=W0231
        """
        @param idx: The attribute name used to index the classads with.
        @keyword attrlist: A list of attributes to record; if it is empty, then
           parse all attributes.
        """
        if not attrlist:
            self.attrlist = []
        else:
            self.attrlist = list(attrlist)
        if isinstance(idx, types.TupleType) and self.attrlist:
            for name in idx:
                if name not in self.attrlist:
                    self.attrlist.append(name)
        if self.attrlist and idx not in self.attrlist:
            self.attrlist.append(idx)
        self.idxAttr = idx
        self.caInfo = {}
        self.attrInfo = ''
        # Initialize some used class variables.
        self._starttime = time.time()
        self._endtime = time.time()
        self._elapsed = 0
        self.curCaInfo = {}
        self.attrName = ''
        
    def startDocument(self):
        """
        Start up a parsing sequence; initialize myself.
        """
        self.attrInfo = ''
        self.caInfo = {}
        self._starttime = time.time()
   
    def endDocument(self):
        """
        Print out debugging information from this document parsing.
        """
        self._endtime = time.time()
        self._elapsed = self._endtime - self._starttime
        myLen = len(self.caInfo)
        log.info("Processed %i classads in %.2f seconds; %.2f classads/" \
                 "second" % (myLen,
                             self._elapsed, myLen/(self._elapsed+1e-10)))

    def startElement(self, name, attrs):
        """
        Open an XML element - take note if its a 'c', for the start of a new
        classad, or an 'a', the start of a new attribute.
        """
        if name == 'c':
            self.curCaInfo = {}
        elif name == 'a':
            self.attrName = str(attrs.get('n', 'Unknown'))
            self.attrInfo = ''
        else:
            pass

    def endElement(self, name):
        """
        End of an XML element - save everything we learned
        """
        if name == 'c':
            if isinstance(self.idxAttr, types.TupleType):
                full_idx = ()
                for idx in self.idxAttr:
                    idx = self.curCaInfo.get(idx, None)
                    if idx:
                        full_idx += (idx,)
                if len(full_idx) == len(self.idxAttr):
                    self.caInfo[full_idx] = self.curCaInfo
            else:
                idx = self.curCaInfo.get(self.idxAttr, None)
                if idx:
                    self.caInfo[idx] = self.curCaInfo
        elif name == 'a':
            if self.attrName in self.attrlist or len(self.attrlist) == 0:
                self.curCaInfo[self.attrName] = self.attrInfo
        else:
            pass

    def characters(self, ch):
        """
        Save up the XML characters found in the attribute.
        """
        self.attrInfo += str(ch)

    def getClassAds(self):
        """
        Returns a dictionary of dictionaries consisting of all the classAds
        and their attributes.
        """
        return self.caInfo

def parseCondorXml(fp, handler): #pylint: disable-msg=C0103
    """
    Parse XML from Condor.

    Create a SAX parser with the content handler B{handler}, then parse the
    contents of B{fp} with it.

    @param fp: A file-like object of the Condor XML data
    @param handler: An object which will be our content handler.
    @type handler: xml.sax.handler.ContentHandler
    @returns: None
    """
    parser = make_parser()
    parser.setContentHandler(handler)
    try:
        parser.setFeature(feature_external_ges, False)
    except xml.sax._exceptions.SAXNotRecognizedException:
        pass
    
    try:
        parser.parse(fp)
    except SAXParseException, e:
        if e.getMessage() == 'no element found':
            pass
        else:
            raise

def condorCommand(command, cp, info=None): #pylint: disable-msg=W0613
    """
    Execute a command in the shell.  Returns a file-like object
    containing the stdout of the command

    Use this function instead of executing directly (os.popen); this will
    allow you to hook your providers into the testing framework.

    @param command: The command to execute
    @param cp: The GIP configuration object
    @keyword info: A dictionary-like object for Python string substitution
    @returns: a file-like object.
    """

    # must test for empty dict for special cases like the condor_status
    #  command which has -format '%s' arguments.  Python will try to do
    #  the string substitutions regardless of single quotes
    if info:
        cmd = command % info
    else:
        cmd = command
    log.debug("Running command %s." % cmd)

    return runCommand(cmd)

