
"""
Process handling routines for the GIP
"""

import os
import pwd
import sys
import time
import errno
import fcntl
import select
import signal
import struct

from gip_common import getLogger, gipDir, cp_get
from gip.utils.process_tracking import start_tracker, PROCESS_TRACKING_BINARY

try:
    #python 2.5 and above  
    import hashlib as md5
except ImportError:
    # pylint: disable-msg=F0401
    import md5

log = getLogger("GIP.ProcessHandling")

process_tracker_pipe = -1

def register_pid(pid):
    if process_tracker_pipe >= 0:
        pid_struct = struct.pack("I", pid)
        os.write(process_tracker_pipe, pid_struct)

def handle_privs(cp):
    """
    Check if we are root.  If we are, drop privileges to gip.gip_user to avoid
    permissions problems when CEMon tries to run the GIP.  Default to 'tomcat'.

    If we are root and the process-tracking executable is available, we'll
    launch a process-tracked child that will listen on a pipe; write a PID
    into the pipe, and the child will launch a new process tracker.
    """
    gip_user = cp_get(cp, "gip", "gip_user", "tomcat")
    start_uid = os.getuid()
    if start_uid != 0:
        return

    # Launch a process-tracked child.
    if os.access(PROCESS_TRACKING_BINARY, os.X_OK):
        read_pipe, write_pipe = os.pipe()
        fcntl.fcntl(read_pipe, fcntl.F_SETFL, fcntl.FD_CLOEXEC)
        fcntl.fcntl(write_pipe, fcntl.F_SETFL, fcntl.FD_CLOEXEC)
        if os.fork() == 0:
            try:
                os.close(write_pipe)
                start_tracker(read_pipe, os.getppid())
            finally:
                os._exit(1)
        os.close(read_pipe)
        global process_tracker_pipe
        process_tracker_pipe = write_pipe

    # NOTE:  Must set gid first or you will get an "Operation not permitted"
    # error
    try:
        pwd_tuple = pwd.getpwnam(gip_user)
        pw_uid = pwd_tuple[2]
        pw_gid = pwd_tuple[3]
        
        os.setregid(pw_gid, pw_gid)
        os.setreuid(pw_uid, pw_uid)
    except:
        # the username was invalid (logging has not been set up yet)
        # Note: we can't log because if we log as root then the ownership of the
        #       log files can potentially get messed up
        print >> sys.stderr, "Invalid username configured: %s" % gip_user
        raise

    return write_pipe

def run_child(executable, output):
    """
    Run a child process, hooking its stdout to output FD, and
    its stderr to the gip logfile.

    If available, uses process-tracking to guarantee any children started
    will be killed off.

    Returns the child PID or throws an exception.
    """
    logdir = gipDir(os.path.expandvars('$GIP_LOCATION/var/logs'), '/var/log/gip')
    stderr_name = os.path.join(logdir, "gip.stderr")
    fd = open(stderr_name, "a")
    fd_in = open("/dev/null", "r")
    pid = os.fork()
    if not pid:
        try:
            _, exec_name = os.path.split(executable)
            os.dup2(fd_in.fileno(), 0)
            os.dup2(output, 1)
            os.dup2(fd.fileno(), 2)
            os.execl(executable, exec_name)
        except:
            os._exit(1)
    register_pid(pid)
    return pid

class WaitPids(object):

    def __init__(self):
        self.r_pipe = -1
        self.w_pipe = -1
        self.sigchld_installed = False

    def __del__(self):
        if self.r_pipe >= 0:
            os.close(self.r_pipe)
        if self.w_pipe >= 0:
            os.close(self.w_pipe)
        # Note this is not thread-safe
        if self.sigchld_installed:
            try:
                self.unregister_sigchld()
            except:
                pass

    def register_sigchld(self):
        """
        Register the SIGCHLD handler.
        When a SIGCHLD is recieved, "1" is written to the internal pipe.
        """

        self.r_pipe, self.w_pipe = os.pipe()
        fcntl.fcntl(self.r_pipe, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(self.w_pipe, fcntl.F_SETFL, os.O_NONBLOCK)
        def wait_pid_sigchld(signum, frame):
             try:
                 os.write(self.w_pipe, "1")
             except:
                 pass
        signal.signal(signal.SIGCHLD, wait_pid_sigchld)
        self.sigchld_installed = True

    def unregister_sigchld(self):
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        self.sigchld_installed = False

    def _reap_pids(self, pid_dict):
        """
        Reap any children possible, noting status in pid_dict.
        Will not throw an exception.  Will not block.
        """
        while True:
            try:
                pid, exit_status = os.waitpid(0, os.WNOHANG)
            except OSError, oe:
                if oe.errno == errno.ECHILD:
                    # No other children exist - we are done.
                    return pid_dict
                elif oe.errno == errno.EINTR:
                    continue
                raise 
            if pid == 0:
                break 
            if pid in pid_dict:
                log.debug("Child %d has exited %d." % (pid, exit_status))
                pid_dict[pid] = exit_status

    def _all_done(self, pid_dict):
        """
        See if all PIDs in pid_dict have an exit status.
        """
        count = 0
        for val in pid_dict.values():
                    if val == -1:
                        count += 1
        return count == 0

    def wait_pids(self, pids, timeout):
        """
        Given a list of PIDs, wait until all exit.
        Returns a dictionary; the key is the PID, the value is the exit status.
        If this function timed out, the exit status will be set to -1.
        """
        pid_dict = dict([(i, -1) for i in pids])

        # Clean up any PIDs that exited before the error handler registered.
        self._reap_pids(pid_dict)
        if self._all_done(pid_dict):
            return pid_dict

        self.register_sigchld()
        remaining = timeout
        r = []
        while remaining >= 0:
            start_time = time.time()
            # When a child dies, the SIGCHLD handler will write into the
            # pipe and select will fire.
            try:
                r, w, x = select.select([self.r_pipe], [], [], timeout)
            except select.error, e:
                # If the SIGCHLD fires while inside the select, we will get
                # an EINTR error.
                if e.args[0] != errno.EINTR:
                    raise
            remaining -= time.time() - start_time
            if not r:
                continue
            # Process as many SIGCHLDs as bytes exist in the pipe.
            while True:
                try:
                    result = os.read(self.r_pipe, 1)
                except OSError, oe:
                    if oe.errno == errno.EINTR:
                        # Interrupted by a signal
                        continue
                    elif oe.errno == errno.EAGAIN:
                        # No more signals to handle.
                        break
                if result != "1":
                    break
                # Reap as many children as possible
                self._reap_pids(pid_dict)
            if self._all_done(pid_dict):
                break
        self.unregister_sigchld()
        return pid_dict

def wait_children(pids, timeout):
    """
    Wait until all of the specified children PIDs exit, or for the specified
    number of seconds to elapse.

    Any child not exited by the timeout will be forcibly killed.

    @param pids: A list of PIDs to monitor.
    @param timeout: Timeout for this function; wait_children will not take
        longer than this (in seconds) to run.
    @returns: A dictionary; key is the PID, value is the exit status.  All
        PIDs in the input will be in this dictionary and have a valid exit
        status.
    """
    if not pids:
        return {}
    wp = WaitPids()
    result = wp.wait_pids(pids, timeout)
    for pid, val in result.items():
        if val == -1:
            log.info("Hard-killing process %d after timeout has elapsed." % pid)
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError, oe:
                # We catch an error because the PID may have already exited.
                # We don't worry about the PID being recycled because if it is
                # dead, it is still a zombie.
                pass
            exit_status = os.waitpid(pid, 0)
            result[pid] = exit_status[1]
    return result
    

def launch_modules(modules, module_dir, temp_dir):
    """
    Launch any module which does not have cached output available.

    This process forks off one child per module.
    The child process writes its output into::
        
        $temp_dir/$name.ldif.tmp

    @param modules: The modules dictionary.  The launched PID of the module
       will be added to its dictionary.
    @param temp_dir: The temporary directory.
    @returns: A list of child PIDs.
    """
    pids = []
    for module, info in modules.items():
        if 'output' in info:
            continue
        filename = os.path.join(temp_dir, '%(name)s.ldif.tmp' % info)
        if os.path.exists(filename):
            os.unlink(filename)
        fd_num = os.open(filename, os.O_WRONLY | os.O_TRUNC | os.O_CREAT | os.O_EXCL)
        executable = os.path.join(module_dir, module)
        pid = run_child(executable, fd_num)
        log.debug("Child %s is running in pid %i" % (module, pid))
        pids.append(pid)
        info['pid'] = pid
    return pids

def list_modules(dirname):
    """
    List all of the modules in a directory.

    The returned directory contains the following keys, one per module:
        - B{name}: The module's name

    @param dirname: Directory to check
    @returns: A dictionary of module data; one key per file in the directory.
    """
    info = {}
    for filename in os.listdir(dirname):
        if os.path.isdir(filename):
            continue
        if filename.startswith('.'):
            continue
        
        # ignore temporary files         
        if filename.endswith('~') or \
               (filename.startswith('#') and filename.endswith('#')):
            continue
        
        mod_info = {}
        mod_info['name'] = filename
        info[filename] = mod_info
        log.debug("Found module %s in directory %s" % (filename, dirname))
    return info

