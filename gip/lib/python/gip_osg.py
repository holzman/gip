"""
Populate the GIP based upon the values from the OSG configuration
"""

import os
import sys
import ConfigParser

from gip_sections import *

site_sec = "Site Information"
pbs_sec = "PBS"
condor_sec = "Condor"
sge_sec = 'SGE'
storage_sec = 'Storage'
gip_sec = 'GIP'

def cp_getInt(cp, section, option, default):
    """
    Helper function for ConfigParser objects which allows setting the default.
    Returns an integer, or the default if it can't make one.

    @param cp: ConfigParser object
    @param section: Section of the config parser to read
    @param option: Option in section to retrieve
    @param default: Default value if the section/option is not present.
    @returns: Value stored in the CP for section/option, or default if it is
        not present.
    """
    try:
        return int(str(cp_get(cp, section, option, default)).strip())
    except:
        return default

def cp_get(cp, section, option, default):
    """
    Helper function for ConfigParser objects which allows setting the default.
    
    ConfigParser objects throw an exception if one tries to access an option
    which does not exist; this catches the exception and returns the default
    value instead. 

    This function is also found in gip_common, but is replicated here to avoid
    circular dependencies.
    
    @param cp: ConfigParser object
    @param section: Section of config parser to read
    @param option: Option in section to retrieve
    @param default: Default value if the section/option is not present.
    @returns: Value stored in CP for section/option, or default if it is not
        present.
    """ 
    try:
        return cp.get(section, option)
    except:
        return default


def configOsg(cp):
    """
    Given the config object from the GIP, overwrite data coming from the
    OSG configuration file.
    """
    # If override, then gip.conf overrides the config.ini settings
    try:
        override = cp.getboolean("gip", "override")
    except:
        override = False

    # See if we have a special config.ini location
    loc = cp_get(cp, "gip", "osg_config", "$VDT_LOCATION/monitoring/config.ini")
    loc = os.path.expandvars(loc)
    # Load config.ini values
    cp2 = ConfigParser.ConfigParser()
    cp2.read(loc)

    # The write_config helper function
    def __write_config(section2, option2, section, option):
        """
        Helper function for config_compat; should not be called directly.

        To avoid circular dependencies, this is a copy of the __write_config in
        gip_common
        """
        try:
            new_val = cp2.get(section2, option2)
        except:
            return
        if not cp.has_section(section):
            cp.add_section(section)
        if override and (not cp.has_option(section, option)):
            cp.set(section, option, new_val)
        elif (not override):
            cp.set(section, option, new_val)

    # Now, we compare the two - convert the config.ini options into gip.conf
    # options.
    # [Site Information]
    __write_config(site_sec, "host_name", ce, "name")
    __write_config(site_sec, "host_name", ce, "unique_name")
    __write_config(site_sec, "site_name", site, "name")
    __write_config(site_sec, "site_name", site, "unique_name")
    __write_config(site_sec, "sponsor", site, "sponsor")
    __write_config(site_sec, "site_policy", site, "sitepolicy")
    __write_config(site_sec, "contact", site, "contact")
    __write_config(site_sec, "email", site, "email")
    __write_config(site_sec, "city", site, "city")
    __write_config(site_sec, "country", site, "country")
    __write_config(site_sec, "longitude", "site", "longitude")
    __write_config(site_sec, "latitude", "site", "latitude")
    
    # [PBS]
    __write_config(pbs_sec, "pbs_location", pbs, "pbs_path")
    __write_config(pbs_sec, "wsgram", pbs, "wsgram")
    __write_config(pbs_sec, "enabled", pbs, "enabled")
    __write_config(pbs_sec, "job_contact", pbs, "contact_string")

    # [Condor]
    __write_config(condor_sec, "condor_location", condor, "condor_location")
    __write_config(condor_sec, "wsgram", condor, "wsgram")

    # [SGE]
    __write_config(sge_sec, "sge_location", sge, "sge_path")
    __write_config(sge_sec, "sge_location", sge, "sge_root")
    __write_config(sge_sec, "wsgram", sge, "wsgram")

    # [Storage]
    __write_config(storage_sec, "app_dir", "osg_dirs", "app")
    __write_config(storage_sec, "data_dir", "osg_dirs", "data")
    __write_config(storage_sec, "worker_node_temp", "osg_dirs", "wn_tmp")

    # [GIP]
    __write_config(gip_sec, "se_name", "se", "name")
    __write_config(gip_sec, "se_host", "se", "unique_name")
    __write_config(gip_sec, "dynamic_dcache", "se", "dynamic_dcache")
    __write_config(gip_sec, "srm", "se", "srm_present")
    __write_config(gip_sec, "batch", "ce", "job_manager")

    # Storage stuff 
    __write_config(gip_sec, "se_control_version", "se", "srm_version")
    __write_config(gip_sec, "srm_version", "se", "srm_version")
    __write_config(gip_sec, "advertise_gsiftp", "classic_se", "advertise_se")

    # Calculate the default path for each VO
    root_path = cp_get(cp2, gip_sec, "se_root_path", "/")
    vo_dir = cp_get(cp2, gip_sec, "vo_dir", "VONAME").replace("VONAME", "$VO")
    default = os.path.join(root_path, vo_dir)
    if not gip_sec in cp2.sections():
        cp2.add_section(gip_sec)
    cp2.set(gip_sec, "default_path", default)
    __write_config(gip_sec, "default_path", "vo", "default")

    # Override the default path with any specific paths.
    vo_dirs = cp_get(cp, gip_sec, 'special_vo_dir', 'UNAVAILABLE')
    if vo_dirs.lower().strip() != "unavailable":
        for path in vo_dirs.split(';'):
            try:
                vo, dir = path.split(',')
            except:
                continue
            cp2.set(gip_sec, "%s_path" % vo, dir)
            __write_config(gip_sec, "%s_path" % vo, "vo", vo)

    # Handle the subclusters.
    sc_naming = {\
        "outbound": "outbound_network",
        "name":     "name",
        "numlcpus": "cores_per_node",
        "nodes":    "node_count",
        "numcpus":  "cpus_per_node",
        "clock":    "cpu_speed_mhz",
        "ramsize":  "ram_size",
        "vendor":   "cpu_vendor",
        "model":    "cpu_model",
        "inbound":  "inbound_network",
    }
    sc_number = cp_getInt(cp2, gip_sec, "sc_number", "0")
    if sc_number == 0:
        sc_number = 0
        for option in cp2.options(gip_sec):
            if option.startswith("sc_name"):
                sc_number += 1
    for idx in range(1, sc_number+1):
        sec = "subcluster_%i" % idx
        for key, val in sc_naming.items():
            __write_config(gip_sec, "sc_%s_%i" % (key, idx), sec, val)
        if not cp.has_section(sec):
            continue
        nodes = cp_getInt(cp, sec, "node_count", "0")
        cpus_per_node = cp_getInt(cp, sec, "cpus_per_node",2)
        cores_per_node = cp_getInt(cp, sec, "cores_per_node", cpus_per_node*2)
        cp.set(sec, "cores_per_cpu", "%i" % (cores_per_node/cpus_per_node))
        cp.set(sec, "total_cores", "%i" % (nodes*cores_per_node))
        cp.set(sec, "total_cpus", "%i" % (nodes*cpus_per_node))

    cp2.set(gip_sec, "bdii", "ldap://is.grid.iu.edu:2170")
    cp2.set(gip_sec, "tmp_var", "True")
    __write_config(gip_sec, "bdii", "bdii", "endpoint")
    __write_config(gip_sec, "tmp_var", "cluster", "simple")
    __write_config(gip_sec, "tmp_var", "cesebind", "simple")

