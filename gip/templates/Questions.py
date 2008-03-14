
config_ce = """
Does this site have a Compute Element (CE) you would like to configure?
"""

config_se = """
Does this site have a Storage Element (SE) you would like to configure?
"""

config_sc = """
Information about your SubClusters
----------------------------------
A subcluster represents a homogeneous collection of nodes within a cluster.
A typical cluster contains only 1 subcluster (i.e. all the nodes are identical)
however some clusters contain more than 1 type of node.  These clusters
have multiple subclusters.

How many subclusters would you like to configure?
"""

start_sc = """
We will now start configuring your subclusters.  If you make any mistakes,
you will be given the option of correcting them after all subclusters have
been configured.
"""

pick_sc = """
Would you like to edit any of your subclusters?
"""

save_progress = """
Would you like to save your progress (y/n)?
"""

save_progress_done = """
Configuration is done.  Would you like to save the results (y/n)?
"""

post_save = """
Configuration saved.  If you would like to alter any choices without 
re-running this configuration script, you may find these answers in:

$VDT_LOCATION/gip/etc/%s
"""

batch = """
Specify your BATCH system (condor/pbs/lsf/sge) 
"""

standalone_gftp = """
Would you like to advertise a stand-alone gridftp server? (Y/n):
"""

sa_intro = """
Information about this storage area (SA).
-------------------------------------------------------------------------------

The storage area information is used to determine which directories a VO can
use on your storage element.  Each storage area has a "root path" which is the
root of the storage area and a "vo path" which a specific VO can write into
relative to the root.

For example, if the root path is "/mnt/grid_storage" and the VO path for CMS
is "cms", CMS will write into "/mnt/grid_storage/cms".

To configure this, we will first set up a root path and default all VOs 
"vo path" to be $VO_NAME, and you may set up individual exceptions.
"""

sa_root_path = """
Please enter the root path for this storage area:
"""

sa_has_exceptions = """
Are there any exceptions you would like to include?
"""

sa_vo_exception = """
What VO should be changed?
"""

sa_exception_path = """
What is the full path for %(vo)s (relative to /)?
"""

sa_more_exceptions = """
Are there any more exceptions?
"""

se_name = """
What is the name of your SE?
"""

se_hostname = """
What is the hostname you would like your SE to be associated with?
(Usually, the name of your SRM endpoint.)
"""

se_tech = """
What SRM implementation are you using?
"""

dcache_automate = """
The GIP has the ability to automate the information gathering from the dCache
storage element.  Would you like to utilize this or would you like to configure
this by hand?
"""

confirm_site = """
The following values for your site have already been set.  To alter them,
exit this script and please re-run $VDT_LOCATION/monitoring/configure-osg.sh

 - OSG_HOSTNAME (Value of your CEUniqueID): %(OSG_HOSTNAME)s
 - OSG_DEFAULT_SE (Your site's CloseSE): %(OSG_DEFAULT_SE)s
 - OSG_SITE_NAME: %(OSG_SITE_NAME)s
 - OSG_SITE_CITY: %(OSG_SITE_CITY)s
 - OSG_SITE_COUNTRY: %(OSG_SITE_COUNTRY)s
 - OSG_SITE_LONGITUDE: %(OSG_SITE_LONGITUDE)s
 - OSG_SITE_LATITUDE: %(OSG_SITE_LATITUDE)s
 - OSG_APP: %(OSG_APP)s
 - OSG_DATA: %(OSG_DATA)s
 - OSG_WN_TMP: %(OSG_WN_TMP)s
 - OSG_JOB_MANAGER (Job manager for your site): %(OSG_JOB_MANAGER)s

"""

explain_dynamic = """
The GIP can auto-configure dCache SEs by utilizing the admin interface and
the SRM Postgres database.  This allows your site to use the GIP advertising
to its fullest.  

It will also allow you report accurate space usage information to the OSG.

The CE will need access to the following resources:
   * dCache admin interface
   * Postgres server on the SRM node.
You may need to configure your database and/or firewall to allow this.

Would you like to continue (y/n) :
"""

admin_start = """
We will now begin the configuration of the dCache admin interface connection.
"""

admin_test = """
We will now test the dCache admin interface.
"""

admin_failure = """
Failed to connect to the admin interface!  The command used was:

%s

Please test this on your own from this node.  Would you like to 
reconfigure (y/n)? 
"""

admin_success = """
Admin connection succeeded!
"""

admin_host = """
What is the hostname of your dCache admin interface?
"""

admin_port = """
What port is the admin interface running on?
"""

admin_user = """
What is the admin user name?
"""

admin_password = """
What is the admin password?
"""

db_start = """
We will now begin the configuration of the Postgres DB connection.
"""

db_missing_bindings = """
You do not appear to have the Postgres-Python bindings (psycopg2) installed.
This component will not be functional until they are installed.
"""

db_test = """
We will now test the Postgres DB connection.
"""

db_failure = """
The DB connection failed.  The command attempted was:

%s

Please try on your own.
Would you like to reconfigure (y/n)?
"""

db_success = """
The DB connection succeeded.
"""

db_host = """
What is the hostname of your SRM Postgres DB?
"""

db_port = """
What DB port should be used?
"""

db_db = """
What is the SRM database name?
"""

db_user = """
What is the SRM database user?
"""

db_passwd = """
What is the password for the SRM database?
"""

