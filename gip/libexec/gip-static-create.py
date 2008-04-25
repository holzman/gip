#!/usr/bin/env python

"""
gip-static-create: Configure the generic information provider

THIS IMPLEMENTATION IS NOT YET COMPLETE.

Original version by Laurence.Field@cern.ch
Complete rewrite by Brian Bockelman
"""

raise NotImplementedError()

import os
import glob
import time
import signal

try:
   import md5
except:
   import hashlib as md5

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getLogger, cp_get

log = getLogger("GIP.Wrapper")

def main():
    cp = config()
    temp_dir = os.path.expandvars(cp_get("gip", "temp_dir", \
        "$VDT_LOCATION/gip/var/tmp")
    plugin_dir = os.path.expandvars(cp_get("gip", "plugin_dir", \
        "$VDT_LOCATION/gip/plugins")
    provider_dir = os.path.expandvars(cp_get("gip", "provider_dir", \
        "$VDT_LOCATION/gip/providers")
    static_dir = os.path.expandvars(cp_get("gip", "static_dir", \
        "$VDT_LOCATION/gip/var/ldif")
    delete_attributes = os.path.expandvars(cp_get("gip", "remove_attributes", \
        "$GIP_LOCATION/etc/remove-attributes.conf")
    freshness = int(cp_get("gip", "freshness", 300)
    cache_ttl = int(cp_get("gip", "cache_ttl", 600)
    response  = int(cp_get("gip", "response",  60)
    timeout = int(cp_get("gip",   "timeout",   150)

    os.setpgrp()
    static_info = read_static()
    providers = list_modules(provider_dir)
    plugins = list_modules(plugin_dir)
    check_cache(providers, temp_dir, freshness)
    check_cache(plugins, temp_dir, freshness)
    pids = launch_modules(providers, temp_dir, timeout)
    pids += launch_modules(plugins, temp_dir, timeout)
    wait_children(response)
    check_cache(providers, temp_dir, cache_ttl)
    check_cache(plugins, temp_dir, cache_ttl)

    handle_providers(static_info, providers)
    handle_plugins(static_info, plugins)

def wait_children(response):
    """
    Wait for any children of this process.

    Blocks until all children are dead or $response seconds have passed.
    """
    def handler(signum, frame):
        raise Exception("Response timed out")
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(int(response))
    os.wait(-1)
    signal.alarm(0)

def launch_modules(modules, temp_dir, timeout):
    """
    Launch any module which does not have cached output available.

    This process forks off one child per module, sets a distinct process group
    for that module.  If the child process lasts more than $timeout seconds,
    it will kill itself.  The child process writes its output into::
        
        $temp_dir/$name.ldif.$cksum.$process_group

    @param modules: The modules dictionary
    @param temp_dir: The temporary directory.
    @param timeout: The timeout value for the child process before it kills
       itself
    @returns: A list of child PIDs.
    """
    pids = []
    for module, info in modules.items():
        if 'output' in info:
            continue
        filename = os.path.join(temp_dir, '%(name)s.ldif.%(cksum)s' % info)
        exec = os.path.join(temp_dir, module)
        pid = os.fork()
        if pid == 0:
            run_child(exec, filename, timeout)
        else:
            pids.append(pid)
    return pids

def check_cache(modules, temp_dir, freshness):
    """
    Check the cache directory for a recent version of module's output.

    The modules dictionary is expected to have keys equal to filenames and
    the values should be a dictionary with the following keys:
    
       - B{cksum}: Checksum of the module's script
       - B{name}: Name of the module

    If sufficiently recent output is discovered, then the function populates 
    the B{output} key with the file contents.

    @param modules: Dictionary containing info for the modules
    @param temp_dir: Directory where module output is saved
    @param freshness: If a file is older than $freshness seconds, its contents
       are ignored.
    """
    for mod in modules:
        filename = os.path.join(temp_dir, "%(name)s.ldif.%(cksum)s", \
            modules[mod])
        if not os.path.exists(filename)
            continue
        try:
            mtime = os.stat(filename)
        except:
            continue
        if mtime + int(freshness) > time.time():
            try:
                fp = open(filename, 'r')
            except:
                continue
            modules['output'] = fp.read()

def list_modules(dirname):
    """
    List all of the modules in a directory.

    The returned directory contains the following keys, one per module:
        - B{name}: The module's name
        - B{cksum}: The module's checksum.

    @param dirname: Directory to check
    @returns: A dictionary of module data; one key per file in the directory.
    """
    info = {}
    for file in os.listdir(dirname):
         mod_info = {}
         mod_info['name'] = file
         mod_info['cksum'] = calculate_hash(os.path.join(dirname, file))
         info[file] = mod_info
    return info

def run_child(exec, filename, timeout):
    pgrp = os.getpgrp()
    filename = '%s.%s' % (filename, pgrp)
    def handler(signum, frame):
        os.unlink(filename)
        os.killpg(-signal.SIGKILL, pgrp)
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout)
    os.system("%s > %s" % (exec, filename))
    signal.alarm(0)
    os._exit(os.EX_OK)

def calculate_hash(filename):
    """
    Calculate the MD5 hash of a file.
    
    @param filename: Filename of file to hash.
    @returns: Digest string of the file's contents.
    """
    m = md5.md5()
    m.update(open(filename, 'r').read())
    return m.digest()

def read_static():
    """
    Read all the files in static_dir, collating the response into a single
    stream.
    """
    streams = ''
    for filename in glob.glob("%s/*.ldif"):
        try:
            info = open(filename, 'r').read()
        except:
            log.error("Unable to read %s." % filename)
        if info[-1] != "\n":
            info += '\n'
        if info[-2] != "\n":
            info += '\n'
        streams += info
    return streams

if __name__ == '__main__':
    main()

