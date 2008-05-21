#!/usr/bin/env python

"""
osg-info-wrapper: Configure the generic information provider

THIS IMPLEMENTATION IS NOT YET COMPLETE.

Original version by Laurence.Field@cern.ch
Complete rewrite by Brian Bockelman
"""

import os
import sys
import glob
import time
import signal
import cStringIO

try:
   import md5
except:
   import hashlib as md5

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getLogger, cp_get
from gip_ldap import read_ldap, compareDN

log = getLogger("GIP.Wrapper")

def main(cp = None, return_entries=False):
    """
    Main method for the osg-info-wrapper script.  This script safely runs the
    plugin and provider modules, caching where necessary, and combines it with
    the static data.  It then outputs the final GLUE information for this site.
    """
    if cp == None:
        cp = config()
    temp_dir = os.path.expandvars(cp_get(cp, "gip", "temp_dir", \
        "$VDT_LOCATION/gip/var/tmp"))
    plugin_dir = os.path.expandvars(cp_get(cp, "gip", "plugin_dir", \
        "$VDT_LOCATION/gip/plugins"))
    provider_dir = os.path.expandvars(cp_get(cp, "gip", "provider_dir", \
        "$VDT_LOCATION/gip/providers"))
    static_dir = os.path.expandvars(cp_get(cp, "gip", "static_dir", \
        "$VDT_LOCATION/gip/var/ldif"))
    delete_attributes = os.path.expandvars(cp_get(cp, "gip", \
        "remove_attributes", "$GIP_LOCATION/etc/remove-attributes.conf"))
    flush_cache = cp_get(cp, "gip", "flush_cache", "False")
    flush_cache = flush_cache.lower().find('t') >= 0
    freshness = int(cp_get(cp, "gip", "freshness", 300))
    cache_ttl = int(cp_get(cp, "gip", "cache_ttl", 600))
    response  = int(cp_get(cp, "gip", "response",  60))
    timeout = int(cp_get(cp, "gip",   "timeout",   150))

    os.setpgrp()
    static_info = read_static()
    providers = list_modules(provider_dir)
    plugins = list_modules(plugin_dir)
    check_cache(providers, temp_dir, freshness)
    check_cache(plugins, temp_dir, freshness)
    pids = launch_modules(providers, provider_dir, temp_dir, timeout)
    pids += launch_modules(plugins, plugin_dir, temp_dir, timeout)
    wait_children(pids, response)
    check_cache(providers, temp_dir, cache_ttl)
    check_cache(plugins, temp_dir, cache_ttl)

    static_fp = cStringIO.StringIO(static_info)
    entries = read_ldap(static_fp, multi=True)
    entries = handle_providers(entries, providers)
    entries = handle_plugins(entries, plugins)
    if return_entries:
        return entries
    for entry in entries:
        print entry.to_ldif()

def handle_providers(entries, providers):
    """
    Add the output from the providers to the list of the GIP entries.

    This will match the DNs; if two DNs are repeated, then one will be thrown
    out

    @param entries: A list of LDIFData objects
    @param providers: A list of provider information dictionaries.
    @returns: The altered entries list.
    """
    provider_entries = []
    for provider, p_info in providers.items():
        if 'output' in p_info:
            fp = cStringIO.StringIO(p_info['output'])
            provider_entries += read_ldap(fp, multi=True)
    remove_entries = []
    # Calculate all the duplicate entries, build a list of the ones
    # to remove.
    for entry in entries:
        for p_entry in provider_entries:
            if compareDN(entry, p_entry):
                remove_entries.append(entry)
    for entry in remove_entries:
        entries.remove(entry)
    # Now add all the new entries from the providers
    for p_entry in provider_entries:
        entries.append(p_entry)
    return entries

def handle_plugins(entries, plugins):
    # Make a list of all the plugin GLUE entries
    plugin_entries = []
    for plugin in plugins:
        if 'modules' in plugin:
            fp = cStringIO.StringIO(provider['modules'])
            plugin_entries += read_ldap(fp, multi=True)
    # Merge all the plugin entries into the main stream.
    for entry in entries:
        for p_entry in plugin_entries:
            if compareDN(entry, p_entry):
                for glue, value in p_entry.glue.items():
                    entry[glue] = value
    return entries

def wait_children(pids, response):
    """
    Wait for any children of this process.

    Blocks until all children are dead or $response seconds have passed.

    @param pids: A list of process IDs to wait on.
    @param response: The maximum number of seconds to wait on child processes.
    @returns: A list of child PIDs which have not been reaped (you should call
        os.wait on these PIDs some time in the future).
    """
    def handler(signum, frame):
        raise Exception("Response timed out")
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(int(response))
    for pid in pids:
        os.waitpid(pid, 0)
    signal.alarm(0)

def launch_modules(modules, module_dir, temp_dir, timeout):
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
        executable = os.path.join(module_dir, module)
        pid = os.fork()
        if pid == 0:
            run_child(executable, filename, timeout)
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
    for mod, mod_info in modules.items():
        if 'output' in mod_info:
            continue
        filename = os.path.join(temp_dir, "%(name)s.ldif.%(cksum)s" % \
            mod_info)
        if not os.path.exists(filename):
            continue
        try:
            my_stat = os.stat(filename)
        except:
            continue
        try:
            mtime = my_stat.st_mtime
        except:
            mtime = my_stat[8]
        if mtime + int(freshness) > time.time():
            try:
                fp = open(filename, 'r')
            except:
                continue
            mod_info['output'] = fp.read()

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
         if os.path.isdir(file):
             continue
         mod_info = {}
         mod_info['name'] = file
         try:
             mod_info['cksum'] = calculate_hash(os.path.join(dirname, file))
         except Exception, e:
             log.exception(e)
         info[file] = mod_info
    return info

def run_child(executable, orig_filename, timeout):
    pgrp = os.getpgrp()
    filename = '%s.%s' % (orig_filename, pgrp)
    def handler(signum, frame):
        os.unlink(filename)
        os.killpg(-signal.SIGKILL, pgrp)
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout)
    exit_code = os.system("%s > %s" % (executable, filename))
    signal.alarm(0)
    if exit_code == 0:
        os.rename(filename, orig_filename)
    os._exit(os.EX_OK)

def calculate_hash(filename):
    """
    Calculate the MD5 hash of a file.
    
    @param filename: Filename of file to hash.
    @returns: Digest string of the file's contents.
    """
    m = md5.md5()
    m.update(open(filename, 'r').read())
    return m.hexdigest()

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

