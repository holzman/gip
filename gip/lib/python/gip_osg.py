"""
Populate the GIP based upon the values from the OSG configuration
"""

import ConfigParser

from gip_sections import *

site_sec = "Site Information"
pbs_sec = "PBS"
condor_sec = "Condor"
sge_sec = 'SGE'
storage_sec = 'Storage'
gip_sec = 'GIP'

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
    
    # [PBS]
    __write_config(pbs_sec, "pbs_location", pbs, "pbs_path")
    __write_config(pbs_sec, "wsgram", pbs, "wsgram")
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

