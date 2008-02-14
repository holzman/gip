
import os
import ConfigParser
import sys
import re

from UserDict import UserDict

import gip_testing
from gip_testing import runCommand

# This evaluates to true if Python 2.3 or higher is
# available.
py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3
if py23:
    import optparse
    import logging
    import logging.config

# Default log level for our FakeLogger object.
loglevel = "info"

def check_gip_location():
    if "GIP_LOCATION" not in os.environ:
        vdt_loc = os.path.expandvars("$VDT_LOCATION/lcg")
        if "VDT_LOCATION" in os.environ:
            if os.path.exists(vdt_loc):
                os.environ["GIP_LOCATION"] = vdt_loc
            else:
                raise ValueError("The $GIP_LOCATION variable is not set and " \
                    "$VDT_LOCATION/lcg does not exist.  Please set the " \
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
        gip_testing.replace_command = True

def config(*args):
    """
    Load up the config file.  It's taken from the command line, option -c
    or --config; default is gip.conf
    """
    check_gip_location()
    check_testing_environment()
    cp = ConfigParser.ConfigParser()
    files = list(args)
    if py23:
        p = optparse.OptionParser()
        p.add_option('-c', '--config', dest='config', \
            help='Configuration file.', default='gip.conf')
        (options, args) = p.parse_args()
        files += [i.strip() for i in options.config.split(',')]
    files += ["$GIP_LOCATION/etc/gip.conf"]
    files = [os.path.expandvars(i) for i in files]
    cp.read(files)
    
    # Set up the config object to be compatible with the OSG attributes
    # file.
    config_compat(cp)
    return cp

def config_compat(cp):
    """
    Currently, all of the configuration information is kept in 
        $VDT_LOCATION/monitoring/osg-attributes.conf
    This function will take in the ConfigParser object `cp` and update it
    with the configurations found from the OSG monitoring.

    If gip.override=True, then the config object overrides the OSG settings.
    If not, then the OSG settings override.

    If VDT_LOCATION is not defined, this function does nothing.
    """
    if "VDT_LOCATION" not in os.environ:
        return

    try:
        override = cp.getboolean("gip", "override")
    except:
        override = False
    osg = None
    try:
        osg = Attributes("$VDT_LOCATION/monitoring/osg-attributes.conf")
    except Exception, e:
        log.error("Unable to open OSG attributes: %s" % str(e))
        osg = None

    if osg != None:
        # Write the attributes from the flat attributes file to the
        # ConfigParser object, which is organized by sections.
        __write_config(cp, override, osg["OSG_HOSTNAME"], "ce", "name")
        __write_config(cp, override, osg["OSG_DEFAULT_SE"], "se", "name")
        __write_config(cp, override, osg["OSG_SITE_NAME"], "site", "name")
        __write_config(cp, override, osg["OSG_SITE_CITY"], "site", "city")
        __write_config(cp, override, osg["OSG_SITE_COUNTRY"], "site", "country")
        __write_config(cp, override, osg["OSG_SITE_LONGITUDE"], "site",
                       "longitude")
        __write_config(cp, override, osg["OSG_SITE_LATITUDE"], "site",
                       "latitude")
        __write_config(cp, override, osg["OSG_APP"], "osg_dirs", "app")
        __write_config(cp, override, osg["OSG_DATA"], "osg_dirs", "data")
        __write_config(cp, override, osg["OSG_WN_TMP"], "osg_dirs", "wn_tmp")
        __write_config(cp, override, osg["OSG_JOB_MANAGER"], "ce", 
                       "job_manager")

    # Do the same but with the gip stuff.
    try:
        gip = Attributes("$VDT_LOCATION/monitoring/gip-attributes.conf")
    except Exception, e:
        log.error("Unable to open GIP attributes: %s" % str(e))
        return

    if gip["OSG_GIP_SIMPLIFIED_SRM"].lower() == "y":
        #simple_path = os.path.join(gip["OSG_GIP_SIMPLIFIED_SRM_PATH"], "$VO")
        simple_path = gip["OSG_GIP_SIMPLIFIED_SRM_PATH"]
        __write_config(cp, override, simple_path, "vo", "default")
    for key in gip.keys():
        if key.startswith("OSG_GIP_VO_DIR"):
            vo, dir = gip[key].split(',')
            __write_config(cp, override, dir, "vo", vo)

def __write_config(cp, override, new_val, section, option):
    """
    Helper function for config_compat; should not be called directly.
    """
    if override and (not cp.has_option(section, option)):
        cp.set(section, option, new_val)
    elif (not override):
        cp.set(section, option, new_val)

def getOsgAttributes():
    """
    Return a dictionary-like object containing the OSG attributes as configured
    by the configure-osg.sh script and stored in the 
    $VDT_LOCATION/monitoring/osg-attributes.conf file.
    """
    return Attributes("$VDT_LOCATION/monitoring/osg-attributes.conf")

class VoMapper:
    """
    This class maps a username to VO.
    """

    def __init__(self, cp):
        self.cp = cp
        try:
            self.map_location = cp.get("vo", "user_vo_map")
        except:
            self.map_location = "$VDT_LOCATION/monitoring/osg-user-vo-map.txt"
        self.voi = []
        self.voc = []
        self.userMap = {}
        self.voMap = {}
        self.parse()
    

    def parse(self):
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
                    self.userMap[user] = vo
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                pass
        for i in range(len(self.voi)):
            try:
                 self.voMap[self.voi[i]] = self.voc[i]
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                pass

    def __getitem__(self, username):
        try: 
            return self.voMap[self.userMap[username]]
        except KeyError:
            raise ValueError("Unable to map user: %s" % username)

def FakeLogger():
    """
    Super simple logger for python installs which don't have the logging
    package.
    """
    def debug(msg, *args):
        print >> sys.stderr, msg

    def info(msg, *args):
        print >> sys.stderr, msg

    def warning(msg, *args):
        print >> sys.stderr, msg

    def error(msg, *args):
        print >> sys.stderr, msg

    def exception(msg, *args):
        print >> sys.stderr, msg

if py23:        
    try:        
        logging.config.fileConfig(os.path.expandvars("$VDT_LOCATION/lcg/etc/" \
            "logging.conf"))
    except:         
        pass

def getLogger(name):
    """
    Returns a logger object corresponding to `name`.
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

def HMSToMin(hms):
    """
    Helper function to convert something of the form HH:MM:SS to number of 
    minutes.
    """
    h, m, s = hms.split(':')
    return int(h)*60 + int(m) + int(round(int(s)/60.0))

class Constants:
        def __init__(self):
                self.CR = '\r'
                self.LF = '\n'

class Attributes(UserDict):
        def __init__(self, attribute_file):
                UserDict.__init__(self, dict=None)
                self.constants = Constants()
                self.load_attributes(attribute_file)

        def load_attributes(self, attribute_file):
                f = open(os.path.expandvars(attribute_file))
                s = f.read()
                e = s.split(self.constants.LF)
                if (len(e[len(e)-1]) == 0): e.pop()

                # Look for lines that match the pattern "key=value"
                # this will also stip out quotation marks
                test = re.compile('^(.*)=(.*)')
                for line in e:
                        valid = test.match(line)
                        if valid:
                                grp = line.split("=")
                                self[grp[0]] = grp[1][1:len(grp[1]) - 1]

def getTemplate(template, name):
    fp = open(os.path.expandvars("$GIP_LOCATION/etc/templates/%s" % template))
    start_str = "dn: %s" % name
    buffer = ''
    recording = False
    for line in fp:
        if line.startswith(start_str):
            recording = True
        if recording:
            buffer += line
            if line == '\n':
                break
    if not recording:
        raise ValueError("Unable to find %s in template %s" % (name, template))
    return buffer[:-1]

