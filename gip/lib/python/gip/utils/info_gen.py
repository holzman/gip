
import os
import glob
import time
import tempfile
import cStringIO

try:
    #python 2.5 and above  
    import hashlib as md5
except ImportError:
    # pylint: disable-msg=F0401
    import md5

from gip_common import config, getLogger, cp_get, cp_getBoolean, cp_getInt, gipDir
from gip_ldap import read_ldap, compareDN, LdapData
import gip_sets as sets

log = getLogger("GIP.InfoGen")

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

def calculate_hash(filename):
    """
    Calculate the MD5 hash of a file.
    
    @param filename: Filename of file to hash.
    @returns: Digest string of the file's contents.
    """
    m = md5.md5()
    m.update(open(filename, 'r').read())
    return m.hexdigest()

def check_cache(modules, temp_dir, freshness):
    """
    Check the cache directory for a recent version of module's output.

    The modules dictionary is expected to have keys equal to filenames and
    the values should be a dictionary with the following keys:
    
       - B{name}: Name of the module

    If sufficiently recent output is discovered, then the function populates 
    the B{output} key with the file contents.

    @param modules: Dictionary containing info for the modules
    @param temp_dir: Directory where module output is saved
    @param freshness: If a file is older than $freshness seconds, its contents
       are ignored.
    """
    for mod_info in modules.values():
        if 'output' in mod_info:
            continue
        filename = os.path.join(temp_dir, "%(name)s.ldif" % \
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

def merge_cache(modules, pid_results, temp_dir, cache_ttl):
    """
    Merge the results of the providers into the temp directory.

    Only modules that ran successfully will be merged.  Each LDIF stanza in the
    output will be marked with an expiration date, determined by cache_ttl.
    """
    birthday = time.time()
    expiry = str(int(birthday + cache_ttl))
    for module, mod_info in modules.items():
        mod_info.setdefault("pid", 0)
        if mod_info['pid'] not in pid_results:
            # Module was not run; must still have valid results.
            continue
        result = pid_results[mod_info['pid']]
        filename = os.path.join(temp_dir, "%(name)s.ldif.tmp" % mod_info)
        if result != 0:
            log.warning("Ignoring results of %s due to non-zero exit status %d." % (module, result))
            try:
                os.unlink(filename)
            except OSError, oe:
                log.warning("Unable to remove output of failed module %s: %s" % (filename, oe.strerror))
            continue
        # Load up output from the modules.
        try:
            fp = open(filename, "r")
            os.unlink(filename)
        except IOError, ie:
            log.warning("Got error when opening cache file %s: %s" % (filename, ie.strerror))
            continue
        except OSError, oe:
            log.warning("Error when deleting tmp file %s: %s" % (filename, oe.strerror))
        try:
            entries = read_ldap(fp, multi=True)
        except IOError, ie:
            log.warning("Got error when reading cache file %s: %s" % (filename, ie.strerror))
        # Add the cache expiration time.
        for entry in entries:
             entry.nonglue['GIPExpiration'] = (expiry,)

        # Now, write out into the final location.
        try:
            fd, tmp_filename = tempfile.mkstemp(dir=temp_dir, prefix="%(name)s.ldif" % mod_info)
        except OSError, oe:
            log.warning("Got error when opening merged tmp file %s: %s" % (tmp_filename, oe.strerror))
            continue
        try:
            for entry in entries:
                os.write(fd, entry.to_ldif())
                os.write(fd, "\n")
        except OSError, oe:
            log.warning("Got error when writing merged tmp file %s: %s" % (tmp_filename, oe.strerror))
            os.unlink(tmp_filename)
            os.close(fd)
            continue
        final_filename = os.path.join(temp_dir, "%(name)s.ldif" % mod_info)
        try:
            os.fdatasync(fd)
            os.rename(tmp_filename, final_filename)
            os.close(fd)
        except OSError, oe:
            log.warning("Error renaming merged file to final location %s: %s" % (final_filename, oe.strerror))
            os.unlink(tmp_filename)
            continue

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
    for _, plugin_info in plugins.items():
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
    for _, p_info in providers.items():
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

    for entry in sets.Set(remove_entries):
        log.debug("Removing entry %s" % entry)  
        try:
            entries.remove(entry)
        except ValueError:
            pass
    # Now add all the new entries from the providers
    for p_entry in provider_entries:
        entries.append(p_entry)
    return entries

def flush_cache(temp_dir):
    """
    Flush the cache by removing the contents of temp_dir.

    @param temp_dir: The temporary directory to delete.
    """
    files = os.listdir(temp_dir)
    for filename in files:
        if filename.startswith('.'):
            continue
        filename = os.path.join(temp_dir, filename)
        try:
            os.remove(filename)
        except:
            log.warn("Unable to flush cache file %s" % filename)

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

