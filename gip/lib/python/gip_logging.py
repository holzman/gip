import os
import sys
import traceback

py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3

if py23:
    import logging, logging.config, logging.handlers

# Default log level for our FakeLogger object.
loglevel = "info"
log = None

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

def add_giplog_handler(log_file='$GIP_LOCATION/var/logs/gip.log'):
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

    logfile = os.path.expandvars(log_file)
    formatter = logging.Formatter('%(asctime)s %(name)s:%(levelname)s ' \
        '%(pathname)s:%(lineno)d:  %(message)s')
    handler = logging.handlers.RotatingFileHandler(logfile,
        maxBytes=1024*1024*10, backupCount=5)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    mylog.addHandler(handler)

def getLogger(name):
    """
    Returns a logger object corresponding to `name`.

    @param name: Name of the logger object.
    """
    if not py23:
        return FakeLogger()
    else:
        return logging.getLogger(name)

def logConfig(log_config, log_file):
    if py23:
        try:
            logging.config.fileConfig(os.path.expandvars(log_config))
            add_giplog_handler()
        except:
            traceback.print_exc(file=sys.stderr)
