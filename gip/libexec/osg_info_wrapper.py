#!/usr/bin/env python

"""
osg-info-wrapper: Configure the generic information provider

Original version by Laurence.Field@cern.ch
Complete rewrite by Brian Bockelman
"""

import os
import sys
import glob
import time
import shutil
import signal
import cStringIO

py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3
if not py23:
      os.EX_CANTCREAT = 73
      os.EX_CONFIG = 78
      os.EX_DATAERR = 65
      os.EX_IOERR = 74
      os.EX_NOHOST = 68
      os.EX_NOINPUT = 66
      os.EX_NOPERM = 77
      os.EX_NOUSER = 67
      os.EX_OK = 0
      os.EX_OSERR = 71
      os.EX_OSFILE = 72
      os.EX_PROTOCOL = 76
      os.EX_SOFTWARE = 70
      os.EX_TEMPFAIL = 75
      os.EX_UNAVAILABLE = 69
      os.EX_USAGE = 64
                                                   
try:
   import md5
except:
   import hashlib as md5

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, getLogger, cp_get, cp_getBoolean, cp_getInt
from gip_ldap import read_ldap, compareDN, LdapData

log = getLogger("GIP.Wrapper")

def create_if_not_exist(*paths):
    """
    Create a directories if they do not exist
    """
    for path in paths:
        # Bail out if it already exists
        if os.path.exists(path):
            continue
        log.info("Creating directory %s because it doesn't exist." % path)
        try:
            os.makedirs(path)
        except Exception, e:
            log.error("Unable to make necessary directory, %s" % path)
            log.exception(e)
            raise

def main(cp = None, return_entries=False):
    """
    Main method for the osg-info-wrapper script.  This script safely runs the
    plugin and provider modules, caching where necessary, and combines it with
    the static data.  It then outputs the final GLUE information for this site.
    """
    log.debug("Starting up the osg-info-wrapper.")
    if cp == None:
        cp = config()
    temp_dir = os.path.expandvars(cp_get(cp, "gip", "temp_dir", \
        "$GIP_LOCATION/var/tmp"))
    plugin_dir = os.path.expandvars(cp_get(cp, "gip", "plugin_dir", \
        "$GIP_LOCATION/plugins"))
    provider_dir = os.path.expandvars(cp_get(cp, "gip", "provider_dir", \
        "$GIP_LOCATION/providers"))
    static_dir = os.path.expandvars(cp_get(cp, "gip", "static_dir", \
        "$GIP_LOCATION/var/ldif"))

    # Make sure that our directories exist.
    create_if_not_exist(temp_dir, plugin_dir, provider_dir, static_dir)

    # Load up our add, alter, and delete attributes
    add_attributes = os.path.expandvars(cp_get(cp, "gip", \
        "add_attributes", "$GIP_LOCATION/etc/add-attributes.conf"))
    alter_attributes = os.path.expandvars(cp_get(cp, "gip", \
        "alter_attributes", "$GIP_LOCATION/etc/alter-attributes.conf"))
    remove_attributes = os.path.expandvars(cp_get(cp, "gip", \
        "remove_attributes", "$GIP_LOCATION/etc/remove-attributes.conf"))

    # Flush the cache if appropriate
    do_flush_cache = cp_getBoolean(cp, "gip", "flush_cache", False)
    if do_flush_cache:
        log.info("Flushing cache upon request.")
        flush_cache(temp_dir)

    # Load up our parameters
    freshness = cp_getInt(cp, "gip", "freshness", 300)
    cache_ttl = cp_getInt(cp, "gip", "cache_ttl", 600)
    response  = cp_getInt(cp, "gip", "response",  60)
    timeout = cp_getInt(cp, "gip",   "timeout",   150)

    os.setpgrp()

    # First, load the static info
    static_info = read_static(static_dir)

    # Discover the providers and plugins
    providers = list_modules(provider_dir)
    plugins = list_modules(plugin_dir)

    # Load up anything in the cache
    check_cache(providers, temp_dir, freshness)
    check_cache(plugins, temp_dir, freshness)

    # Launch the providers and plugins
    pids = launch_modules(providers, provider_dir, temp_dir, timeout)
    pids += launch_modules(plugins, plugin_dir, temp_dir, timeout)

    # Wait for the results
    wait_children(pids, response)

    # Load results from the cache
    check_cache(providers, temp_dir, cache_ttl)
    check_cache(plugins, temp_dir, cache_ttl)

    # Create LDAP entries out of the static info
    static_fp = cStringIO.StringIO(static_info)
    entries = read_ldap(static_fp, multi=True)

    # Apply output from the providers
    entries = handle_providers(entries, providers)

    # Apply output from the plugins
    entries = handle_plugins(entries, plugins)

    # Finally, apply our special cases
    entries = handle_add_attributes(entries, add_attributes)
    entries = handle_alter_attributes(entries, alter_attributes)
    entries = handle_remove_attributes(entries, remove_attributes)

    # Return the LDAP or print it out.
    if return_entries:
        return entries
    for entry in entries:
        print entry.to_ldif()

def flush_cache(temp_dir):
    """
    Flush the cache by removing the contents of temp_dir.

    @param temp_dir: The temporary directory to delete.
    """
    files = os.listdir(temp_dir)
    for file in files:
        if file.startswith('.'):
            continue
        file = os.path.join(temp_dir, file)
        try:
            os.remove(file)
        except:
            log.warn("Unable to flush cache file %s" % file)

def handle_providers(entries, providers):
    """
    Add the output from the providers to the list of the GIP entries.

    This will match the DNs; if two DNs are repeated, then one will be thrown
    out

    @param entries: A list of LdapData objects
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

def handle_add_attributes(entries, add_attributes):
    """
    Handle the add_attributes file, a special case of a provider.

    The contents of the add attributes file are treated as the output of a
    provider.  The add attributes file is applied after all other providers.

    @param entries: A list of LdapData objects
    @param add_attributes: The name of the desired add_attributes file.
    @returns: The entries list with the new attributes
    """
    if not os.path.exists(add_attributes):
        log.warning("The add-attributes.conf file does not exist.")
        return entries
    try:
        output = open(add_attributes).read()
    except Exception, e:
        log.error("An exception occurred when trying to read the " \
            "add-attributes file %s" % add_attributes)
        log.exception(e)
        return entries
    info = {'add_attributes': {'output': output}}
    return handle_providers(entries, info)

def handle_alter_attributes(entries, alter_attributes):
    """
    Handle the alter_attributes file, a special case of a plugin.

    The contents of the alter attributes file are treated as the output of a
    plugin.  The alter attributes file is applied after all other plugins.

    @param entries: A list of LdapData objects
    @param alter_attributes: The name of the desired alter_attributes file.
    @returns: The entries list with the altered attributes
    """
    if not os.path.exists(alter_attributes):
        log.warning("The alter-attributes.conf file does not exist.")
        return entries
    try:
        output = open(alter_attributes).read()
    except Exception, e:
        log.error("An exception occurred when trying to read the " \
            "alter-attributes file %s" % alter_attributes)
        log.exception(e)
        return entries
    info = {'alter_attributes': {'output': output}}
    return handle_plugins(entries, info)

def handle_remove_attributes(entries, remove_attributes):
    """
    Handle the remove_attributes file, which can remove entities from the
    GIP entries

    Each non-blank line of the remove_attributes file is treated as a separate
    DN.  Then, we go through the entries and remove any entries with a matching
    DN.

    @param entries: A list of LdapData objects
    @param remove_attribtues: The name of the desired remove_attributes file.
    @returns: The entries list with the removed attributes
    """
    if not os.path.exists(remove_attributes):
        log.warning("The remove-attributes file %s does not exist." % \
            remove_attributes)
        return entries
    try:
        output = open(remove_attributes).read()
    except Exception, e:
        log.error("An exception occurred when trying to read the " \
            "remove-attributes file %s" % remove_attributes)
        log.exception(e)
        return entries
    log.debug("Successfully opened the remove_attributes file.")

    # Collect all the DNs to be removed
    remove_dns = []
    for line in output.splitlines():
        if len(line.strip()) == 0 or line.startswith('#'):
            continue
        dn = line.strip()
        remove_dns.append(LdapData(dn))
    log.debug("There are %i entries to remove." % len(remove_dns))

    # Match all the entries:
    remove_entries = []
    for entry in entries:
        for dn in remove_dns:
            if compareDN(entry, dn):
                remove_entries.append(entry)

    # Remove all the unwanted entries
    for entry in remove_entries:
        entries.remove(entry)
    
    return entries

def handle_plugins(entries, plugins):
    # Make a list of all the plugin GLUE entries
    plugin_entries = []
    for plugin, plugin_info in plugins.items():
        if 'output' in plugin_info:
            fp = cStringIO.StringIO(plugin_info['output'])
            plugin_entries += read_ldap(fp, multi=True)
    # Merge all the plugin entries into the main stream.
    for p_entry in plugin_entries:
        log.debug("Plugin contents:\n%s" % p_entry)
    for entry in entries:
        for p_entry in plugin_entries:
            if compareDN(entry, p_entry):
                for glue, value in p_entry.glue.items():
                    entry.glue[glue] = value
                for key, value in p_entry.nonglue.items():
                    entry.nonglue[key] = value
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
    log.debug("Setting response time to %.2f" % float(response))
    signal.alarm(int(response))
    for pid in pids:
        try:
            os.waitpid(pid, 0)
        except:
            # Important note: we just quit waiting for children, we don't
            # necessarily kill them off.
            break
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
            log.debug("Child %s is running in pid %i" % (module, pid))
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
        log.debug("Checking cache for file %s" % filename)
        if not os.path.exists(filename):
            log.debug("File does not exist.")
            continue
        try:
            my_stat = os.stat(filename)
        except:
            log.debug("File is too old.")
            continue
        try:
            mtime = my_stat.st_mtime
        except:
            mtime = my_stat[8]
        if mtime + int(freshness) > time.time():
            try:
                fp = open(filename, 'r')
            except Exception, e:
                log.exception(e)
                continue
            log.debug("  Loading file contents.")
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
         if file.startswith('.'):
             continue
         mod_info = {}
         mod_info['name'] = file
         try:
             mod_info['cksum'] = calculate_hash(os.path.join(dirname, file))
         except Exception, e:
             log.exception(e)
         info[file] = mod_info
         log.debug("Found module %s in directory %s" % (file, dirname))
    return info

def run_child(executable, orig_filename, timeout):
    log.info("Running module %s" % executable)
    os.setpgrp()
    pgrp = os.getpgrp()
    filename = '%s.%s' % (orig_filename, pgrp)
    def cleanup_handler(pid):
        try:
            os.unlink(filename)
        except:
            pass
        try:
            os.unlink(orig_filename)
        except:
            pass
        log.warning("The module %s timed out!" % executable)
        log.warning("Attempting to kill pgrp %i" % pgrp)
        try:
            os.kill(pid, signal.SIGKILL)
            os.killpg(pgrp, signal.SIGKILL)
        finally:
            log.warning("Exiting with status %i" % os.EX_SOFTWARE)
            os._exit(os.EX_SOFTWARE)
    log.debug("Set a %.2f second timeout." % timeout)
    t1 = -time.time()
    sys.stderr = open(os.path.expandvars("$GIP_LOCATION/var/logs/module.logs"), 'a')
    exec_name = executable.split('/')[-1]
    pid = os.spawnl(os.P_NOWAIT, "/bin/sh", exec_name, "-c", "%s > %s" % \
        (executable, filename))
    exit_code = None
    while t1+time.time() < timeout:
        time.sleep(.1)
        status = os.waitpid(pid, os.WNOHANG)
        if status[0] == pid: # If subprocess not done, status==(0, 0)
            exit_code = os.WEXITSTATUS(status[1])
            break
    if exit_code == None:
        log.debug("Pid %i, cleaning up" % os.getpid())
        cleanup_handler(pid)
    if exit_code == 0:
        log.debug("Pid %i, finishing successfully" % os.getpid())
        log.info("Executable %s ran successfully." % executable)
        if not os.path.exists(filename):
            log.error("Output file %s does not exist." % filename)
            os._exit(os.EX_DATAERR)
        try:
            os.rename(filename, orig_filename)
        except:
            os._exit(os.EX_CANTCREAT)
    else:
        log.info("Executable %s died with exit code %s" % (executable, \
            str(exit_code)))
        os._exit(exit_code)
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

def read_static(static_dir):
    """
    Read all the files in static_dir, collating the response into a single
    stream.

    Only considers files ending in '.ldif'

    @param static_dir: Directory to look in for static files.
    """
    streams = ''
    for filename in glob.glob("%s/*.ldif" % static_dir):
        log.debug("Reading static file %s" % filename)
        try:
            info = open(filename, 'r').read()
        except:
            log.error("Unable to read %s." % filename)
        if len(info) == 0:
            continue
        if info[-1] != "\n":
            info += '\n'
        if info[-2] != "\n":
            info += '\n'
        streams += info
    return streams

if __name__ == '__main__':
    main()

