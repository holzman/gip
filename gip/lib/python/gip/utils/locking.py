
"""
This module implements POSIX file locking for the GIP.

Original implementation written by Brian Bockelman for Gratia
"""

import os
import sys
import time
import fcntl
import atexit
import struct

from gip_common import cp_get, getLogger, gipDir

log = getLogger("GIP.Locking")

fd = None
# Record the PID that initially took the lock, to prevent unlinking
# when a fork'ed child exits.
pid_with_lock = None
def close_and_unlink_lock():
    global fd
    global pid_with_lock
    if fd:
        if pid_with_lock == os.getpid():
            os.unlink(fd.name)
        fd.close()
atexit.register(close_and_unlink_lock)

def exclusive_lock(cp, timeout):
    """
    Grabs an exclusive lock on /var/lock/gip.lock

    If the lock is owned by another process, and that process is older than the
    timeout, then the other process will be signaled.  If the timeout is
    negative, then the other process is never signaled.

    If we are unable to hold the lock, this call will not block on the lock;
    rather, it will throw an exception.

    The location of the lock can be overridden using the config parameter
    gip.lockfile
    """

    default_location_dir = gipDir(os.path.expandvars('$GIP_LOCATION/var'),
        '/var/lock')
    default_location = os.path.join(default_location_dir, "gip.lock")
    lock_location = cp_get(cp, "gip", "lockfile", default_location)

    lock_location = os.path.abspath(lock_location)
    lockdir = os.path.dirname(lock_location)
    if not os.path.isdir(lockdir):
        raise Exception("Lock is to be created in a directory %s which does "
            "not exist." % lockdir)

    global fd
    global pid_with_lock
    fd = open(lock_location, "w")

    # POSIX file locking is cruelly crude.  There's nothing to do besides
    # try / sleep to grab the lock, no equivalent of polling.
    # Why hello, thundering herd.

    # An alternate would be to block on the lock, and use signals to interupt.
    # This would mess up Gratia's flawed use of signals already, and not be
    # able to report on who has the lock.  I don't like indefinite waits!
    max_tries = 5
    for tries in range(1, max_tries+1):
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fd.write("%d" % os.getpid())
            pid_with_lock = os.getpid()
            fd.flush()
            return
        except IOError, ie:
            if not ((ie.errno == errno.EACCES) or (ie.errno == errno.EAGAIN)):
                raise
            if check_lock(fd, timeout):
                time.sleep(.2) # Fast case; however, we have *no clue* how
                               # long it takes to clean/release the old lock.
                               # Nor do we know if we'd get it if we did
                               # fcntl.lockf w/ blocking immediately.  Blech.
                # Check again immediately, especially if this was the last
                # iteration in the for loop.
                try:
                    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fd.write("%d" % os.getpid())
                    pid_with_lock = os.getpid()
                    fd.flush()
                    return
                except IOError, ie:
                    if not ((ie.errno == errno.EACCES) or (ie.errno == errno.EAGAIN)):
                        raise
        fd.close()
        fd = open(lock_location, "w")
        log.info("Unable to acquire lock, try %i; will sleep for %i "
            "seconds and try %i more times." % (tries, tries, max_tries-tries))
        time.sleep(tries)

    raise Exception("Unable to acquire lock")

def check_lock(fd, timeout):
    """
    For internal use only.

    Given a fd that is locked, determine which process has the lock.
    Kill said process if it is older than "timeout" seconds.
    This will log the PID of the "other process".
    """

    pid = get_lock_pid(fd)
    if pid == os.getpid():
        return True

    if timeout < 0:
        log.info("Another process, %d, holds the probe lockfile." % pid)
        return False

    try:
        age = get_pid_age(pid)
    except:
        log.warning("Another process, %d, holds the probe lockfile." % pid)
        log.warning("Unable to get the other process's age; will not time "
            "it out.")
        return False

    log.info("Another process, %d (age %d seconds), holds the probe "
        "lockfile." % (pid, age))

    if age > timeout:
        os.kill(pid, signal.SIGKILL)
    else:
        return False

    return True

linux_struct_flock = "hhxxxxqqixxxx"
try:
    os.O_LARGEFILE
except AttributeError:
    start_len = "hhlli"

def get_lock_pid(fd):
    # For reference, here's the definition of struct flock on Linux
    # (/usr/include/bits/fcntl.h).
    #
    # struct flock
    # {
    #   short int l_type;   /* Type of lock: F_RDLCK, F_WRLCK, or F_UNLCK.  */
    #   short int l_whence; /* Where `l_start' is relative to (like `lseek').  */
    #   __off_t l_start;    /* Offset where the lock begins.  */
    #   __off_t l_len;      /* Size of the locked area; zero means until EOF.  */
    #   __pid_t l_pid;      /* Process holding the lock.  */
    # };
    #
    # Note that things are different on Darwin
    # Assuming off_t is unsigned long long, pid_t is int
    try:
        if sys.platform == "darwin":
            arg = struct.pack("QQihh", 0, 0, 0, fcntl.F_WRLCK, 0)
        else:
            arg = struct.pack(linux_struct_flock, fcntl.F_WRLCK, 0, 0, 0, 0)
        result = fcntl.fcntl(fd, fcntl.F_GETLK, arg)
    except IOError, ie:
        if ie.errno != errno.EINVAL:
            raise
        log.error("Unable to determine which PID has the lock due to a "
            "python portability failure.  Contact the developers with your"
            " platform information for support.")
        return False
    if sys.platform == "darwin":
        _, _, pid, _, _ = struct.unpack("QQihh", result)
    else:
        _, _, _, _, pid = struct.unpack(linux_struct_flock, result)
    return pid

def get_pid_age(pid):
    now = time.time()
    st = os.stat("/proc/%d" % pid)
    return now - st.st_ctime

