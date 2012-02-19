
import os
import sys
import errno
import fcntl
import select
import signal
import struct

PROCESS_TRACKING_BINARY = "/usr/share/lcmaps-plugins-process-tracking/process-tracking"

def _start_process_tracking(pid, ppid):
    """
    Start the process tracker:
        process-tracker <pid> <ppid>

    This function does not return.
    """
    #log.debug("Starting process tracking for %d." % pid)
    try:
        null_fd = os.open("/dev/null", os.O_WRONLY)
        os.close(0)
        os.dup2(null_fd, 1)
        os.dup2(null_fd, 2)
        os.close(null_fd)
        os.execl(PROCESS_TRACKING_BINARY, "process-tracking",
            str(pid), str(ppid))
    finally:
        print >> sys.stderr, "Exiting due to failure in _start_proces_tracking"
        os._exit(1)

def register_sigchld(w_pipe):
    def sigchld_handler(signum, frame):
        written = False
        while not written:
            try:
                os.write(w_pipe, "1")
            except OSError, oe:
                if oe.errno != errno.EINTR:
                    break
                continue
            written = True
    signal.signal(signal.SIGCHLD, sigchld_handler)

def _start_tracker(r_pipe, grandparent):
    # This process (as root) will listen to the pipe, and start new tracker
    # instances as necessary.
    r2, w2 = os.pipe()
    fcntl.fcntl(r2, fcntl.F_SETFL, fcntl.FD_CLOEXEC | os.O_NONBLOCK)
    fcntl.fcntl(w2, fcntl.F_SETFL, fcntl.FD_CLOEXEC | os.O_NONBLOCK)
    register_sigchld(w2)

    while os.getppid() != 1:
        try:
            r, w, x = select.select([r_pipe, r2], [], [], 1)
        except select.error, e:
            if e.args[0] == errno.EINTR:
                continue
        if r_pipe in r:
            try:
                pid_struct = os.read(r_pipe, 4)
            except OSError, oe:
                if oe.errno == errno.EINTR:
                    continue
            if len(pid_struct) == 0:
                # Parent died and took the pipe with it.
                os._exit(0)
            if len(pid_struct) != 4:
                print >> sys.stderr, "Invalid pipe entry: %s" % pid_struct
                os._exit(1)
            pid = struct.unpack("I", pid_struct)[0]
            if os.fork() == 0:
                _start_process_tracking(pid, grandparent)
        if r2 in r:
            while True:
                try:
                    os.read(r2, 1)
                except OSError, oe:
                    if oe.errno == errno.EINTR:
                        continue
                    elif oe.errno == errno.EAGAIN:
                        break
                    raise
            while True:
                try:
                    pid, status = os.waitpid(0, os.WNOHANG)
                except OSError,oe:
                    if oe.errno == errno.ECHILD:
                        break
                    elif oe.errno == errno.EINTR:
                        continue
                    raise
                if pid == 0:
                    break

def start_tracker(r_pipe, grandparent):
    """
    Start a tracker; if you write an int into the other end of the r_pipe,
    the tracker will start process-tracking.
    This function should not return.

    This function cannot use the logging facilities, as it might cause files
    to rotate as root.
    """
    if os.fork() == 0:
        # Exec child to process-tracking.
        _start_process_tracking(os.getppid(), grandparent)
    try:
        try:
            _start_tracker(r_pipe, grandparent)
        except Exception, e:
            print >> sys.stderr, "Exception in start_tracking:", str(e)
            raise
    finally:
        print >> sys.stderr, "Exiting due to failure in start_tracking while loop"
        os._exit(1)

