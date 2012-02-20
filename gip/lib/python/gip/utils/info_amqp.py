
import os
import socket
import cStringIO

from gip_ldap import read_ldap
from gip_common import cp_get, cp_getBoolean, cp_getInt, getLogger, config, \
    gipDir
from gip.utils.info_gen import merge_cache, handle_plugins, flush_cache, \
    handle_add_attributes, handle_alter_attributes, handle_remove_attributes, \
    check_cache, read_static, handle_providers, calculate_updates
from gip.utils.process_handling import launch_modules, list_modules, wait_children
from gip.utils.info_main import create_if_not_exist
from gip.utils.amqp import connect, send_entries

def main(cp = None, return_entries=False):
    """
    Main method for the osg-info-wrapper script.  This script safely runs the
    plugin and provider modules, caching where necessary, and combines it with
    the static data.  It then outputs the final GLUE information for this site.
    """
    
    if cp == None:
        cp = config()

    log = getLogger("GIP.InfoMain")

    log.debug("Starting up the osg-info-wrapper.")

    temp_dir = os.path.expandvars(cp_get(cp, "gip", "temp_dir", \
        gipDir("$GIP_LOCATION/var/tmp", '/var/cache/gip'))) 
    plugin_dir = os.path.expandvars(cp_get(cp, "gip", "plugin_dir", \
        gipDir("$GIP_LOCATION/plugins", '/usr/libexec/gip/plugins')))
    provider_dir = os.path.expandvars(cp_get(cp, "gip", "provider_dir", \
        gipDir("$GIP_LOCATION/providers", '/usr/libexec/gip/providers')))
    static_dir = os.path.expandvars(cp_get(cp, "gip", "static_dir", \
        gipDir("$GIP_LOCATION/var/ldif", '/etc/gip/ldif.d')))

    # Make sure that our directories exist.
    create_if_not_exist(temp_dir, plugin_dir, provider_dir, static_dir)

    # Load up our add, alter, and delete attributes
    add_attributes = os.path.expandvars(cp_get(cp, "gip", \
        "add_attributes", gipDir("$GIP_LOCATION/etc/add-attributes.conf",
                                 '/etc/gip/add-attributes.conf')))
    alter_attributes = os.path.expandvars(cp_get(cp, "gip", \
        "alter_attributes", gipDir("$GIP_LOCATION/etc/alter-attributes.conf",
                                   '/etc/gip/alter-attributes.conf')))
    remove_attributes = os.path.expandvars(cp_get(cp, "gip", \
        "remove_attributes", gipDir("$GIP_LOCATION/etc/remove-attributes.conf",
                                    '/etc/gip/remove-attributes.conf')))

    # Flush the cache if appropriate
    do_flush_cache = cp_getBoolean(cp, "gip", "flush_cache", False)
    if do_flush_cache:
        log.info("Flushing cache upon request.")
        flush_cache(temp_dir)

    # Load up our parameters
    freshness  = cp_getInt(cp, "gip", "freshness", 60*30)
    static_ttl = cp_getInt(cp, "gip", "static_ttl", 60*60*12)
    cache_ttl  = cp_getInt(cp, "gip", "cache_ttl", 86400*7)
    response   = cp_getInt(cp, "gip", "response",  240)

    try:
        os.setpgrp()
    except OSError, oe:
        # If launched from a batch system (condor), we might not have perms
        if oe.errno != 1:
            raise

    # First, load the static info
    static_info = read_static(static_dir, static_ttl)

    # Discover the providers and plugins
    providers = list_modules(provider_dir)
    plugins = list_modules(plugin_dir)

    # Load up anything in the cache
    check_cache(providers, temp_dir, freshness)
    check_cache(plugins, temp_dir, freshness)

    # Launch the providers and plugins
    pids = launch_modules(providers, provider_dir, temp_dir)
    pids += launch_modules(plugins, plugin_dir, temp_dir)

    # Wait for the results
    results = wait_children(pids, response)

    # Load results from the cache
    merge_cache(providers, results, temp_dir, cache_ttl)
    merge_cache(plugins, results, temp_dir, cache_ttl)

    # Create LDAP entries out of the static info
    static_fp = cStringIO.StringIO(static_info)
    entries = read_ldap(static_fp, multi=True)

    # Apply output from the providers
    entries = handle_providers(entries, providers)

    # Apply output from the plugins
    entries = handle_plugins(entries, plugins)

    # Apply our special cases
    entries = handle_add_attributes(entries, add_attributes, static_ttl)
    entries = handle_alter_attributes(entries, alter_attributes, static_ttl)
    entries = handle_remove_attributes(entries, remove_attributes, static_ttl)

    full_entries, updates = calculate_updates(entries, temp_dir)

    print len(full_entries)
    print len(updates)
    #for update in updates:
    #    print update.to_ldif()

    conn = connect(cp)
    channel = conn.channel()

    # TODO: Better determination of the resource name.
    resource_name = socket.getfqdn()

    send_entries(channel, 'entries.%s' % resource_name, full_entries)
    send_entries(channel, 'updates.%s' % resource_name, updates)

    conn.close()

