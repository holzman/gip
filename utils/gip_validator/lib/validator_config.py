import os
import sys
import re
import types
import ConfigParser

from validator_exceptions import ConfigurationException
from validator_common import runCommand

# This evaluates to true if Python 2.3 or higher is available.
py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3
if py23: import optparse

def interpolateConfig(cp):
    grid = cp_getBoolean(cp, "validator", "itb", False)
    if cp_getBoolean(cp, "validator", "use_oim", False):
        site_list_url = cp_get(cp, "URLS", "site_list", "")
        sitelist_cmd = "wget -O - %s 2>/dev/null | grep \",%s,\" | grep \",CE\" | cut -f1 -d," % (site_list_url, grid)
        sitelist = runCommand(sitelist_cmd).read().split()
        sitelist = ",".join(sitelist)
        cp.set("validator", "site_names", sitelist)
    else:
        if cp_get(cp, "validator", "site_names", "") == "":
            raise ConfigurationException("No mds-vo-name specified and use_oim=False in the config.ini")
    return cp

def parseOpts(args):
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

def isReadable(myfile):
    readable = False
    try:
        open(myfile, 'r')
        readable = True
    except IOError:
        readable = False

    return readable

def config(*args):
    """
    Load up the config file.  It's taken from the command line, option -c
    or --config; default is $GIP_LOCATION/etc/gip.conf

    If python 2.3 is not available, the command line option is not checked.

    If any arguments are supplied to this function, they will be interpreted
    as filenames for additional config files to read.  If the filename
    considers environmental variables, they will be expanded.
    """
    cp = ConfigParser.ConfigParser()
    if py23:
        p = optparse.OptionParser()
        p.add_option('-c', '--config', dest='config', help='Configuration file.', \
                     default='$VALIDATOR_LOCATION/etc/config.ini')
        (options, args) = p.parse_args()
        files = [i.strip() for i in options.config.split(',')]
    else:
        keywordOpts, passedOpts, givenOpts = parseOpts(args)
        if len(keywordOpts["config"]) > 0:
            files = [i.strip() for i in keywordOpts["config"].split(',')]
        elif len(keywordOpts["c"]) > 0:
            files = [i.strip() for i in keywordOpts["c"].split(',')]
        elif len(givenOpts[0]) > 0:
            files = givenOpts
        elif len(passedOpts) > 0:
            files = passedOpts

    files = [os.path.expandvars(i) for i in files]
    files += [os.path.expandvars("$VALIDATOR_LOCATION/etc/config.ini")]
    # Try to read all the files; toss a warning if a config file can't be read:
    for myfile in files:
        isReadable(myfile)
    cp.read(files)
    cp = interpolateConfig(cp)

    return cp

def cp_get(cp, section, option, default=""):
    try:
        return cp.get(section, option)
    except:
        return default

def cp_getBoolean(cp, section, option, default=True):
    val = str(cp_get(cp, section, option, default)).lower()
    if val.find('t') >= 0 or val.find('y') >= 0 or val.find('1') >= 0:
        return True
    if val.find('f') >= 0 or val.find('n') >= 0 or val.find('0') >= 0:
        return False
    return default

def cp_getInt(cp, section, option, default):
    try:
        return int(str(cp_get(cp, section, option, default)).strip())
    except:
        return default

split_re = re.compile("\s*,?\s*")
def cp_getList(cp, section, option, default):
    try:
        results = cp_get(cp, section, option, default)
        if isinstance(results, types.StringType):
            results = split_re.split(results)
        return results
    except:
        return list(default)

def configContents(cp, stream=sys.stderr):
    for section in cp.sections():
        print >> stream, "[%s]" % section
        for option in cp.options(section):
            msg = "   %-25s : %s" % (option, cp.get(section, option))
            print >> stream, msg
        print >> stream, " "
