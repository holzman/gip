
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
from gip_ldap import read_ldap, compareDN, LdapData, ldap_diff, cmpDN
import gip_sets as sets

log = getLogger("GIP.InfoGen")

def read_static(static_dir, static_ttl):
    """
    Read all the files in static_dir, collating the response into a single
    stream.

    Only considers files ending in '.ldif'

    @param static_dir: Directory to look in for static files.
    """
    streams = ''
    for filename in glob.glob("%s/*.ldif" % static_dir):
        log.debug("Reading static file %s" % filename)
        info = _handle_static_file(filename, static_ttl)
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

def write_entries(entries, temp_dir, prefix, final_filename):
    try:
        fd, tmp_filename = tempfile.mkstemp(dir=temp_dir, prefix=prefix)
    except OSError, oe:
        log.warning("Got error when opening file %s: %s" % (tmp_filename, oe.strerror))
        return
    try:
        for entry in entries:
            os.write(fd, entry.to_ldif())
            os.write(fd, "\n")
    except OSError, oe:
        log.warning("Got error when writing to temp file %s: %s" % (tmp_filename, oe.strerror))
        os.unlink(tmp_filename)
        os.close(fd)
        return
    try:
        os.fdatasync(fd)
        os.rename(tmp_filename, final_filename)
        os.close(fd)
    except OSError, oe:
        log.warning("Error renaming temp file to final location %s: %s" % (final_filename, oe.strerror))
        os.unlink(tmp_filename)

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
        final_filename = os.path.join(temp_dir, "%(name)s.ldif" % mod_info)
        write_entries(entries, temp_dir, "%(name)s.ldif.tmp" % mod_info, final_filename)
        mod_info['output'] = "\n".join([i.to_ldif() for i in entries])

def handle_alter_attributes(entries, alter_attributes, static_ttl):
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
    output = _handle_static_file(alter_attributes, static_ttl)
    info = {'alter_attributes': {'output': output}}
    return handle_plugins(entries, info)

def handle_remove_attributes(entries, remove_attributes, static_ttl):
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

    # Copy/paste job from _handle_static_file
    now = time.time()
    st = os.stat(remove_attributes)
    last_update = st.st_mtime
    if last_update - now > static_ttl:
        # Touch the file to fake now as a cache time.
        last_update = now
        try:
            os.utimens(remove_attributes, None)
        except OSError, oe:
            log.error("An exception occurred when trying to touch the " \
                "static file %s" % remove_attributes)

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
    #for p_entry in plugin_entries:
    #    log.debug("Plugin contents:\n%s" % p_entry)
    for entry in entries:
        for p_entry in plugin_entries:
            if compareDN(entry, p_entry):
                for glue, value in p_entry.glue.items():
                    entry.glue[glue] = value
                for key, value in p_entry.nonglue.items():
                    entry.nonglue[key] = value
    return entries

def _handle_static_file(filename, static_ttl):
    """
    Handle a static files.
    Read out the entries, add an expiration time based on the file's mtime.
    If the file is older than static_ttl seconds, update the mtime.

    Return a string containing the LDIF.
    """
    try:
        fp = open(filename, "r")
    except IOError,ie:
        log.error("An exception occurred when trying to open the " \
            "static file %s" % filename)
        log.exception(e)
        return ''
    try:
        entries = read_ldap(fp, multi=True)
    except Exception, e:
        log.error("An exception occurred when trying to read the " \
            "static file %s" % filename)
        log.exception(e)
        return ''
    now = time.time()
    st = os.stat(filename)
    last_update = st.st_mtime
    if last_update - now > static_ttl:
        # Touch the file to fake now as a cache time.
        last_update = now
        try:
            os.utimens(filename, None)
        except OSError, oe:
            log.error("An exception occurred when trying to touch the " \
                "static file %s" % filename)
    expiry = last_update + static_ttl
    for entry in entries:
        entry.nonglue['GIPExpiration'] = (expiry,)
    return '\n'.join([i.to_ldif() for i in entries])

def handle_add_attributes(entries, add_attributes, static_ttl):
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
    output = _handle_static_file(add_attributes, static_ttl)
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

def _read_output_entries(filename):
    if not os.path.exists(filename):
        return []
    try:
        fp = open(filename, 'r')
    except OSError, oe:
        return []
    try:
        return read_ldap(fp, multi=True)
    except OSError, oe:
        return []

def calculate_updates(entries, temp_dir):
    """
    Given a set of entries, split the full updates from the partial updates.

    Check temp_dir/gip_output.ldif for the last full output; each LDIF stanza
    will have a GIPExpiration time; if the expiration time has passed, it is a
    full update.  If the expiration time has not passed, do a delta and only
    return those attributes that have changed.
    """
    full_updates = {}
    partial_updates = {}
    filename = os.path.join(temp_dir, "gip_output.ldif")
    old_entries = _read_output_entries(filename)
    entries_dict = {}
    old_entries_dict = {}
    for entry in entries:
        entries_dict[entry.dn] = entry
    for entry in old_entries:
        old_entries_dict[entry.dn] = entry
    now = time.time()
    for dn, entry in old_entries_dict.items():
        is_expired = int(entry.nonglue['GIPExpiration'][0]) < now
        if (dn not in entries_dict) and (not is_expired):
            full_updates[dn] = entry
        elif dn in entries_dict:
            if is_expired:
                full_updates[dn] = entries_dict[dn]
            else:
                partial_updates[dn] = ldap_diff(entry, entries_dict[dn])

    for dn, entry in entries_dict.items():
        if dn in old_entries_dict:
            continue
        full_updates[dn] = entry

    write_entries(full_updates.values(), temp_dir, "gip_output.ldif", filename)

    return full_updates.values(), partial_updates.values()

ldif_top_str = \
"""
dn: o=grid
objectClass: top
objectClass: GlueTop
objectClass: organization
o: grid

dn: mds-vo-name=local,o=grid
objectClass: GlueTop
objectClass: MDS
objectClass: top
Mds-Vo-name: local
"""

def sort_and_fill(entries):

    # TODO: Figure out a way to remove
    ldif_fp = cStringIO.StringIO(ldif_top_str)
    top_entries = read_ldap(ldif_fp, multi=True)
    entries += top_entries

    entries.sort(cmp=cmpDN)

    return entries
