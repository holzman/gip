
"""
Common functions for GIP providers and plugins.

A set of general-purpose functions to help GIP plugin/provider authors write
probes which are consistent and correct.

This module should generally follow PEP 8 coding guidelines.
"""

__author__ = "Brian Bockelman"

import os
import re
import sys
import pwd
import types
import socket
import traceback
import ConfigParser
import urllib
import tempfile

from UserDict import UserDict

from gip_ldap import read_bdii

#pylint: disable-msg=W0105

# This evaluates to true if Python 2.3 or higher is
# available.
py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3
"""
True if the current version of Python is 2.3 or higher; enables a few extra
capabilities which Python 2.2 does not have.
"""

if py23:
    import optparse, logging, logging.config, logging.handlers

# Default log level for our FakeLogger object.
loglevel = "info"

def check_gip_location():
    """
    This function checks to make sure that GIP_LOCATION is set and exists.
    If GIP_LOCATION is not set and $VDT_LOCATION/gip exists, then it adds::

        GIP_LOCATION=$VDT_LOCATION/gip

    to the process's environment

    It raises ValueErrors if neither situation holds.

    This function is automatically run by the *config* function, so it is
    generally not necessary for provider authors to use this directly.
    """
    if "GIP_LOCATION" not in os.environ:
        vdt_loc = os.path.expandvars("$VDT_LOCATION/gip")
        if "VDT_LOCATION" in os.environ:
            if os.path.exists(vdt_loc):
                os.environ["GIP_LOCATION"] = vdt_loc
            else:
                raise ValueError("The $GIP_LOCATION variable is not set and " \
                    "$VDT_LOCATION/gip does not exist.  Please set the " \
                    "$GIP_LOCATION variable and run again.")
        else:
            raise ValueError("The $GIP_LOCATION and $VDT_LOCATION variables " \
                "are not set; please set $GIP_LOCATION to the proper path and" \
                " run this script again.")
    else:
        loc = os.path.expandvars("$GIP_LOCATION")
        if not os.path.exists(loc):
            raise ValueError("The $GIP_LOCATION variable points to %s, " \
                "which does not exist." % loc)

def check_testing_environment():
    """
    Check to see if the GIP_TESTING environment variable has been set.
    If so, set the gip_testing.replace_command variable to be true.  This causes
    the GIP to read output from static files instead of running the command.
    This is useful if you want to test, say, PBS on a laptop with no PBS
    installed.
    """
    if 'GIP_TESTING' in os.environ:
        __import__('gip_testing').replace_command = True

def parseOpts( args ):
    """
    Parse the passed command line options.
    
    Does not expect the first element of argv; if you pass this directly, use
    parseOpts(sys.argv[1:]).
    
    There are three objects returned:
       - keywordOpts: Options of the form --key=val or -key=val or -key val;
          this is the dictionary of the key: val pairs.
       - passedOpts: Options of the form -name1 or --name2.  List of strings.
       - givenOpts: Options which aren't associated with any flags.  List of 
          strings.
    
    @param args: A list of strings which are the command line options.
    @return: keywordOpts, passedOpts, givenOpts.  See the docstring.
    """
    # Stupid python 2.2 on SLC3 doesn't have optparser...
    keywordOpts = {}
    passedOpts = []
    givenOpts = []
    length = len(args)
    optNum = 0
    while ( optNum < length ):
        opt = args[optNum]
        hasKeyword = False
        if len(opt) > 2 and opt[0:2] == '--':
            keyword = opt[2:]
            hasKeyword = True
        elif opt[0] == '-':
            keyword = opt[1:]
            hasKeyword = True
        if hasKeyword:
            if keyword.find('=') >= 0:
                keyword, value = keyword.split('=', 1)
                keywordOpts[keyword] = value
            elif optNum + 1 == length:
                passedOpts.append( keyword )
            elif args[optNum+1][0] == '-':
                passedOpts.append( keyword )
            else:
                keywordOpts[keyword] = args[optNum+1]
                optNum += 1
        else:
            givenOpts.append( args[optNum] )
        optNum += 1
    return keywordOpts, passedOpts, givenOpts

def config(*args):
    """
    Load up the config file.  It's taken from the command line, option -c
    or --config; default is $GIP_LOCATION/etc/gip.conf

    If python 2.3 is not available, the command line option is not checked.

    If any arguments are supplied to this function, they will be interpreted
    as filenames for additional config files to read.  If the filename
    considers environmental variables, they will be expanded.
    """
    check_gip_location()
    check_testing_environment()
    cp = ConfigParser.ConfigParser()
    files = list(args)
    if py23:
        p = optparse.OptionParser()
        p.add_option('-c', '--config', dest='config', \
            help='Configuration file.', default='gip.conf')
        p.add_option('-f', '--format', dest='format', \
            help='Unittest output format', default='')
        (options, args) = p.parse_args()
        files += [i.strip() for i in options.config.split(',')]
    else:
        keywordOpts, passedOpts, givenOpts = parseOpts(sys.argv)
        if keywordOpts["config"]:
             files += [i.strip() for i in keywordOpts["config"].split(',')]
        if keywordOpts["c"]:
             files += [i.strip() for i in keywordOpts["c"].split(',')]
            
    files = [os.path.expandvars(i) for i in files]
    files += [os.path.expandvars("$GIP_LOCATION/etc/gip.conf")]
    if 'GIP_CONFIG' in os.environ:
        files += [os.path.expandvars("$GIP_CONFIG")]

    log.info("Using GIP SVN revision $Revision$")

    # Try to read all the files; toss a warning if a config file can't be
    # read:
    for myfile in files:
        log.info("Using config file: %s" % myfile)
        try:
            open(myfile, 'r')
        except IOError, ie:
            if ie.errno == 13:
                log.warning("Could not read config file %s due to permissions"%\
                    myfile)

    cp.read(files)

    # Set up the config object to be compatible with the OSG attributes
    # file.
    readOsg = cp_getBoolean(cp, "gip", "read_osg", "True")
    if readOsg:
        from gip_osg import configOsg
        configOsg(cp)

    if 'GIP_DUMP_CONFIG' in os.environ:
        configContents(cp)

    return cp

def __write_config(cp, override, dict_object, key, section, option): \
        #pylint: disable-msg=C0103
    """
    Helper function for config_compat; should not be called directly.
    """
    try:
        new_val = dict_object[key]
    except:
        return
    if not cp.has_section(section):
        cp.add_section(section)
    if override and (not cp.has_option(section, option)):
        cp.set(section, option, new_val)
    elif (not override):
        cp.set(section, option, new_val)

class VoMapper:
    """
    This class maps a username to VO.

    After it loads the username -> VO mapping, it can be used as a dictionary.

    The map_location variable holds the location of the user-to-vo map; this
    defaults to vo.user_vo_map in the config file.  The `parse` method
    re-parses the file.
    """

    def __init__(self, cp):
        self.cp = cp
        try:
            self.map_location = cp.get("vo", "user_vo_map")
        except:
            self.map_location = "$VDT_LOCATION/monitoring/osg-user-vo-map.txt"
        log.info("Using user-to-VO map location %s." % self.map_location)
        self.voi = []
        self.voc = []
        self.userMap = {}
        #self.voMap = {}
        self.parse()


    def parse(self):
        """
        Parse the user-to-vo map specified at `self.map_location`
        """
        fp = open(os.path.expandvars(self.map_location), 'r')
        for line in fp:
            try:
                line = line.strip()
                if line.startswith("#voi"):
                    self.voi = line.split()[1:]
                elif line.startswith("#VOc"):
                    self.voc = line.split()[1:]
                elif line.startswith("#"):
                    continue
                else:
                    user, vo = line.split()
                    if vo.startswith('us'):
                        vo = vo[2:]
                    self.userMap[user] = vo
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                pass
        #for i in range(len(self.voi)):
        #    try:
        #         self.voMap[self.voi[i]] = self.voc[i]
        #    except (KeyboardInterrupt, SystemExit):
        #        raise
        #    except:
        #        pass

    def __getitem__(self, username):
        try:
            return self.userMap[username]
        except KeyError:
            raise ValueError("Unable to map user: %s" % username)

class FakeLogger:
    """
    Super simple logger for python installs which don't have the logging
    package.
    """
    
    def __init__(self):
        pass
    
    def debug(self, msg, *args):
        """
        Pass a debug message to stderr.
        
        Prints out msg % args.
        
        @param msg: A message string.
        @param args: Arguments which should be evaluated into the message.
        """
        print >> sys.stderr, str(msg) % args

    def info(self, msg, *args):
        """
        Pass an info-level message to stderr.
        
        @see: debug
        """
        print >> sys.stderr, str(msg) % args

    def warning(self, msg, *args):
        """
        Pass a warning-level message to stderr.

        @see: debug
        """
        print >> sys.stderr, str(msg) % args

    def error(self, msg, *args):
        """
        Pass an error message to stderr.

        @see: debug
        """
        print >> sys.stderr, str(msg) % args

    def exception(self, msg, *args):
        """
        Pass an exception message to stderr.

        @see: debug
        """
        print >> sys.stderr, str(msg) % args

def add_giplog_handler():
    """
    Add a log file to the default root logger.
    
    Uses a rotating logfile of 10MB, with 5 backups.
    """
    mylog = logging.getLogger()
    try:
        os.makedirs(os.path.expandvars('$GIP_LOCATION/var/logs'))
    except OSError, oe:
        #errno 17 = File Exists
        if oe.errno != 17:
            return
    logfile = os.path.expandvars('$GIP_LOCATION/var/logs/gip.log')
    formatter = logging.Formatter('%(asctime)s %(name)s:%(levelname)s ' \
        '%(pathname)s:%(lineno)d:  %(message)s')
    handler = logging.handlers.RotatingFileHandler(logfile,
        maxBytes=1024*1024*10, backupCount=5)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    mylog.addHandler(handler)

if py23:
    try:
        logging.config.fileConfig(os.path.expandvars("$GIP_LOCATION/etc/" \
            "logging.conf"))
        add_giplog_handler()
    except:
        traceback.print_exc(file=sys.stderr)

def getLogger(name):
    """
    Returns a logger object corresponding to `name`.

    @param name: Name of the logger object.
    """
    if not py23:
        return FakeLogger()
    else:
        return logging.getLogger(name)

log = getLogger("GIP.common")

def addToPath(new_element):
    """
    Add a directory to the path.
    """
    path = os.environ.get("PATH")
    path = "%s:%s" % (str(new_element), path)
    os.environ["PATH"] = path

def HMSToMin(hms): #pylint: disable-msg=C0103
    """
    Helper function to convert something of the form HH:MM:SS to number of
    minutes.
    """
    h, m, s = hms.split(':')
    return int(h)*60 + int(m) + int(round(int(s)/60.0))

def getTemplate(template, name):
    """
    Return a template from a file.

    @param template: Name of the template file in $GIP_LOCATION/templates.
    @param name: Entry in the template file; for now, this is the first
        entry of the DN.
    @return: Template string
    @raise e: ValueError if it is unable to find the template in the file.
    """
    fp = open(os.path.expandvars("$GIP_LOCATION/templates/%s" % template))
    start_str = "dn: %s" % name
    mybuffer = ''
    recording = False
    for line in fp:
        if line.startswith(start_str):
            recording = True
        if recording:
            mybuffer += line
            if line == '\n':
                break
    if not recording:
        raise ValueError("Unable to find %s in template %s" % (name, template))
    return mybuffer[:-1]

def printTemplate(template, info):
    """
    Print out the LDIF contained in template using the values from the
    dictionary `info`.

    The different entries of the template are matched up to keys in the `info`
    dictionary; the entries' values are the dictionary values.

    To see what keys `info` needs for your template, read the template as
    found in::

        $GIP_LOCATION/templates

    @param info: Dictionary of information to fill out for the template.
        The keys correspond to the blank entries in the template string.
    @type info: Dictionary
    @param template: Template string returned from getTemplate.
    """
    populatedTemplate = (template % info).split('\n')
    test = re.compile('.*__GIP_DELETEME.*')
    for line in populatedTemplate:
        deletable = test.match(line)
        if not deletable: print line

def voList(cp, vo_map=None):
    """
    Return the list of valid VOs for this install.  This data is taken from
    the vo mapper and the blacklist / whitelist in the "vo" section of the
    config parser `cp` is applied.

    @param cp: Configuration information for this site.
    @type cp: ConfigParser
    """
    if vo_map == None:
        vo_map = VoMapper(cp)
    vos = []
    for vo in vo_map.userMap.values():
        #vo = vo.lower()
        if vo not in vos:
            vos.append(vo)
    try:
        blacklist = [i.strip() for i in cp.get("vo", "vo_blacklist").split(',')]
    except:
        blacklist = []
    try:
        whitelist = [i.strip() for i in cp.get("vo", "vo_whitelist").split(',')]
    except:
        whitelist = []
    for vo in whitelist:
        #vo = vo.lower()
        if vo not in vos:
            vos.append(vo)
    for vo in blacklist:
        #vo = vo.lower()
        if vo in vos:
            vos.remove(vo)
    return vos

def getHostname():
    """
    Convenience function for retrieving the local hostname.
    """
    return socket.gethostbyaddr(socket.gethostname())

def fileRead(path):
    """
    Return the contents of the file on a given path
    
    @param path: Path to file for reading
    @raise IOError: When file can't be read (doesn't exist, permission errors)
    @return: File contents.
    """
    rFile = open(path)
    return rFile.read()

def fileWrite(path, contents):
    """
    Append some contents to a given path
    
    @param path: Path to file we will append.
    @param contents: Additional contents for file.
    @raise IOError: When file can't be appended.
    """
    wFile = open(path,"a")
    wFile.write(contents)
    wFile.close()

def fileOverWrite(path, contents):
    """
    Overwrite a file with a contents.
    If file doesn't exist, create and write as usual.
    
    @param path: Path to file we will [over]write.
    @param contents: Contents for file.
    @raise IOError: When file can't be [over]written.
    """
    owFile = open(path,"w")
    owFile.write(contents)
    owFile.close()

def cp_get(cp, section, option, default):
    """
    Helper function for ConfigParser objects which allows setting the default.

    ConfigParser objects throw an exception if one tries to access an option
    which does not exist; this catches the exception and returns the default
    value instead.

    @param cp: ConfigParser object
    @param section: Section of config parser to read
    @param option: Option in section to retrieve
    @param default: Default value if the section/option is not present.
    @returns: Value stored in CP for section/option, or default if it is not
        present.
    """
    try:
        return cp.get(section, option)
    except:
        return default

def cp_getBoolean(cp, section, option, default=True):
    """
    Helper function for ConfigParser objects which allows setting the default.

    If the cp object has a section/option of the proper name, and if that value
    has a 'y' or 't', we assume it's supposed to be true.  Otherwise, if it
    contains a 'n' or 'f', we assume it's supposed to be true.
    
    If neither applies - or the option doesn't exist, return the default

    @param cp: ConfigParser object
    @param section: Section of config parser to read
    @param option: Option in section to retrieve
    @param default: Default value if the section/option is not present.
    @returns: Value stored in CP for section/option, or default if it is not
        present.
    """
    val = str(cp_get(cp, section, option, default)).lower()
    if val.find('t') >= 0 or val.find('y') >= 0 or val.find('1') >= 0:
        return True
    if val.find('f') >= 0 or val.find('n') >= 0 or val.find('0') >= 0:
        return False
    return default

def cp_getInt(cp, section, option, default):
    """
    Helper function for ConfigParser objects which allows setting the default.
    Returns an integer, or the default if it can't make one.

    @param cp: ConfigParser object
    @param section: Section of the config parser to read
    @param option: Option in section to retrieve
    @param default: Default value if the section/option is not present.
    @returns: Value stored in the CP for section/option, or default if it is
        not present.
    """
    try:
        return int(str(cp_get(cp, section, option, default)).strip())
    except:
        return default

split_re = re.compile("\s*,?\s*")
def cp_getList(cp, section, option, default):
    """
    Helper function for ConfigParser objects which allows setting the default.
    Returns a list, or the default if it can't make one.

    @param cp: ConfigParser object
    @param section: Section of the config parser to read
    @param option: Option in section to retrieve
    @param default: Default value if the section/option is not present.
    @returns: Value stored in the CP for section/option, or default if it is
        not present.
    """
    try:
        results = cp_get(cp, section, option, default)
        if isinstance(results, types.StringType):
            results = split_re.split(results)
        return results
    except:
        return list(default)

def pathFormatter(path, slash=False):
    """
    Convience function to format a path.
    
    @param path: Path to format
    @keyword slash: Add a trailing (on the right) slash to the path if
      it is not present.
    @return: right-strip the path of all the '/'; if slash=True, then add
      a slash to the end.
    """
    if slash:
        if not (path[-1] == "/"):
            path = path + '/'
    else:
        path = path.rstrip('/')

    return path

def ldap_boolean(val):
    """
    Return a LDIF-formatted boolean.
    """
    if val:
        return "TRUE"
    return "FALSE"

def notDefined(val):
    """
    Returns TRUE if the input value is possibly not defined (i.e., matches
    UNAVAILABLE, UNDEFINED, or UNKNOWN).
    """
    if val.lower() in ['UNAVAILABLE', 'UNDEFINED', 'UNKNOWN']:
        return True
    return False

def normalizeFQAN(fqan):
    """
    Return fqan in the form of /<VO>[/<VO group>/]Role=<VO Role>

    If VO group is not specified, return /<VO>/Role=<VO Role>
    If the VO Role is not specified, return /VO/Role=*
    """
    if fqan.startswith("VOMS:"):
        fqan = fqan[5:]
    if fqan.startswith('VO:'):
        fqan = fqan[3:]
    if not fqan.startswith('/'):
        fqan = '/' + fqan
    if fqan.find('Role=') < 0:
        fqan += '/Role=*'
    return fqan

def matchFQAN(fqan1, fqan2):
    """
    Return True if fqan1 matches with fqan2, False otherwise

    fqan1 may actually be more specific than fqan2.  So, if fqan1 is /cms/blah
    and fqan2 is /cms, then there is a match.  If the Role=* for fqan2, the
    value of the Role for fqan1 is ignored.

    FQANs may be of the form:
       - VOMS:<FQAN>
       - VO:<VO Name>
       - <FQAN>
       - <VO>

    @param fqan1: The FQAN we are testing for match
    @param fqan2: The FQAN 
    """
    fqan1 = normalizeFQAN(fqan1)
    fqan2 = normalizeFQAN(fqan2)
    vog1, vor1 = fqan1.split('/Role=')
    vog2, vor2 = fqan2.split('/Role=')
    vog_matches = False
    vor_matches = False
    if vor2 == '*':
        vor_matches = True
    elif vor2 == vor1:
        vor_matches = True
    if vog1.startswith(vog2):
        vog_matches = True
    return vog_matches and vor_matches

rvf_parse = re.compile('(.+?): (?:(.*?)\n)')
def parseRvf(name):
    """
    Parse the Globus RVF for a specific file.
    
    Retrieves the file from $GLOBUS_LOCATION/share/globus_gram_job_manager/name
    
    See an example RVF file for the patterns this matches.  Returns a 
    dictionary of dictionaries; the keys are attributes, and the values are a 
    dictionary of key: value pairs for all the associated information for an
    attribute.
    
    In the case of an exception, this just returns {}
    
    @param name:  Name of RVF file to parse.
    @return: Dictionary of dictionaries containing information about the 
      attributes in the RVF file; returns an empty dict in case if there is an
      error.
    """
    if 'GLOBUS_LOCATION' in os.environ:
        basepath = os.environ['GLOBUS_LOCATION']
    else:
        basepath = os.environ.get('VDT_LOCATION', '/UNKNOWN')
        basepath = os.path.join(basepath, 'globus')
    fullname = os.path.join(basepath, 'share/globus_gram_job_manager', name)
    if not os.path.exists(fullname):
        return {}
    try:
        rvf = open(fullname, 'r').read()
    except:
        return  {}
    pairs = rvf_parse.findall(rvf)
    curAttr = None
    results = {}
    for key, val in pairs:
        if key == 'Attribute':
            curAttr = val
            results[curAttr] = {}
            continue
        if curAttr == None:
            continue
        results[curAttr][key] = val
    return results

def getURLData(some_url, lines=False):
    """
    Return the data from a URL.
    
    @param some_url: URL to retrieve
    @keyword lines: True to split the content by lines, False otherwise.
    @return: The data of the URL; a string if lines=False and a list of
      lines if lines=True
    """
    data = None
    filehandle = urllib.urlopen(some_url)
    if lines:
        data = filehandle.readlines()
    else:
        data = filehandle.read()

    return data

def getUrlFd(some_url):
    """
    Return a file descriptor to a URL.
    """
    return urllib.urlopen(some_url)

def compare_by(fieldname):
    """
    Returns a function which can be used to compare two dictionaries.
    
    @param fieldname: The dictionaries will be compared by applying cmp
      to this particular keyname.
    @return: A function which can be used to compare two dictionaries.
    """
    def compare_two_dicts (a, b):
        """
        Compare two dictionaries, a and b based on a set field.
        
        @return: cmp(a[fieldname], b[fieldname])
        """
        return cmp(a[fieldname], b[fieldname])
    return compare_two_dicts

def ls(directory):
    """
    Convenience function for os.listdir; returns a directory listing.
    """
    return os.listdir(directory)

def responseTimes(cp, running, waiting, average_job_time=None,
        max_job_time=None):
    """
    Computes the estimated and worst-case response times based on a simple
    formula.

    We take the
      ERT = average_job_time/(running+1)*waiting 
      WRT = max_job_time/(running+1)*waiting 

    If |running| + |waiting| < 10, then ERT=1hr, WRT=24hr unless |running|=0.
    If |running|=0 or |waiting|=0, then ERT=1 min.

    ERT and WRT must be positive; ERT maxes out at 1 day, WRT maxes out
    at 30 days.  WRT must be a minimum of 2*ERT.

    @param cp: Site configuration
    @param running: Number of jobs running
    @param waiting: Number of waiting jobs
    @keyword average_job_time: Average runtime (in seconds) for a job
    @keyword max_job_time: Maximum runtime (in seconds for a job
    @return: ERT, WRT (both are measured in seconds)
    """
    try:
        running = int(running)
    except:
        running = 0
    try:
        waiting = int(waiting)
    except:
        waiting = 0
    try:
        average_job_time = int(average_job_time)
    except:
        average_job_time = None
    try:
        max_job_time = int(max_job_time)
    except:
        max_job_time = None
    if average_job_time == None:
        average_job_time = cp_getInt(cp, 'gip', 'average_job_time', 4*3600)
    if max_job_time == None:
        max_job_time = cp_getInt(cp, 'gip', 'max_job_time', 24*3600)
    if max_job_time < average_job_time:
        max_job_time = 2*average_job_time
    if abs(running) + abs(waiting) < 10:
        if abs(running) == 0 or abs(waiting) == 0:
            return 60, 86400
        return 3600, 86400
    ERT = int(average_job_time/float(running+10)*waiting)
    WRT = int(max_job_time/float(running+1)*waiting)
    ERT = max(min(ERT, 86400), 0)
    WRT = max(min(WRT, 30*86400), 2*ERT)
    return ERT, WRT

def getFQDNBySiteName(cp, sitename):
    fqdn = ""
    entries = read_bdii(cp, query="(objectClass=GlueCE)", base="mds-vo-name=%s,mds-vo-name=local,o=grid" % sitename)
    for entry in entries:
        if 'GlueCE' in entry.objectClass:
            fqdn = entry.glue['CEHostingCluster']
            break
    return fqdn
    
def getTempFilename():
    try:
        conffile = tempfile.NamedTemporaryFile()
        conffile = conffile.name
    except:
        conffile = tempfile.mktemp()
    return conffile

def configContents(cp, stream=sys.stderr):
    for section in cp.sections():
        print >> stream, "[%s]" % section
        for option in cp.options(section):
            msg = "   %-25s : %s" % (option, cp.get(section, option))
            print >> stream, msg
        print >> stream, " "

def strContains(main_str, sub_str):
    result = False
    if py23:
        result = sub_str in main_str
    else:
        contains = lambda haystack, needle: haystack.find(needle) > -1
        if contains(main_str, sub_str) > 0: result = True
    
    return result

def get_user_pwd(name):
    pwd_tuple = pwd.getpwnam(name)
    pwd_dict = {"pw_name"   : pwd_tuple[0],
                "pw_passwd" : pwd_tuple[1],
                "pw_uid"    : pwd_tuple[2],
                "pw_gid"    : pwd_tuple[3],
                "pw_gecos"  : pwd_tuple[4],
                "pw_dir"    : pwd_tuple[5],
                "pw_shell"  : pwd_tuple[6]
               }
    return pwd_dict
