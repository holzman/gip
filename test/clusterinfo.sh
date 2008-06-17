#!/bin/sh

# Script to find out cluster information (clusterinfo.sh)
# Author: Karthik
# Date: May 30th 2008

# PRE-CONDITION: 	
	# Should be running one of condor or pbs or lsf batch systems; Should be able to ssh automatically (without prompted for password) from the CE into other hosts in the cluster
# LIMITATION: 
	# Nodes that are down at the time of executing this script won't be part of the statistics 

# WHERE TO RUN: 	Needs to be run from the CE under the account which would have automatic ssh access to all WNs  
# HOW TO RUN: 		Can be run as a cron job or manually from command line
# HOW OFTEN TO RUN: 	Once in a while - say once a month or so or whenever the cluster configuration changes

###############################################################################################################################

uHosts=/tmp/uhosts.txt # unique hosts 
hostDetails=/tmp/hostDetails.txt # Information about the hosts
hostSummary=/tmp/hostSummary.txt
refresh=0 # 1 = Force a refresh of host information. Set from command line by using -r
help=0    # 1 = User requested for help. Set from command line by using -h 

# Command usage function
usage()
{
        echo "Usage: $0 [-h|-r]"
        echo "          Use -r to force a refresh of the node details."
        echo "          Use -h for help."
}

# Get command line options
while getopts hr option
do
        case $option in
		# -h = Display usage and exit 
                h)
			help=1
			usage
			exit 1
		;;

		# -r = Force a refresh for the host details by moving the files
                r)
			[ -e $uHosts ] && mv $uHosts ${uHosts}.old
			[ -e $hostDetails ] && mv $hostDetails ${hostDetails}.old
			[ -e $hostSummary ] && mv $hostSummary ${hostSummary}.old
			refresh=1
		;;

                *)
			usage
			exit 2
		;;
        esac
done

# Exit if there are any command line arguments (any argument is illegal other than the options above)
[ $help -eq 0 ] && [ $refresh -eq 0 ] && [ $# -gt 0 ] && echo "ERROR: Illegal argument $*" && usage && exit 2

# source the setup.sh file to set batch system and the path for the batch system command to extract host/node information
if [ -e setup.sh ]; then
	if [ -s setup.sh ]; then
		. setup.sh
	else
		echo "ERROR: setup.sh file is empty" 
		echo
		echo "Look for the examples below to see what it should contain"
		echo
		echo "1. For sites using condor"
		echo "BATCH=condor"
		echo "CONDOR_STATUS_PATH=<path to the condor_status command>"
		echo "Example:- CONDOR_STATUS_PATH=/usr/local/bin"
		echo
		echo "2. For sites using pbs"
		echo "BATCH=pbs"
		echo "PBSNODES_PATH=<path to the pbsnodes command>"
		echo "Example:- PBSNODES_PATH=/usr/local/bin"
		echo
		echo "3. For sites using lsf"
		echo "BATCH=lsf"
		echo "BHOSTS_PATH=<path to the bhosts command>"
		echo "Example:- BHOSTS_PATH=/usr/local/bin"
		echo
		exit 2
	fi
else
	echo "ERROR: setup.sh file not found" 
	echo "Create a setup.sh file under `pwd` and try again"
	exit 2
fi

# Find unique hosts in the cluster based on the batch system
case $BATCH in 
	condor) 
		if [ -z $CONDOR_STATUS_PATH ]; then 
			echo "ERROR: CONDOR_STATUS_PATH not set in setup.sh file." && exit 2
		elif [ ! -x $CONDOR_STATUS_PATH/condor_status ]; then
			echo "ERROR: Executable $CONDOR_STATUS_PATH/condor_status doesn't exist. Check the value of the variable CONDOR_STATUS_PATH in setup.sh" && exit 2
		fi
	;;
	lsf) 
		if [ -z $BHOSTS_PATH ]; then 
			echo "ERROR: BHOSTS_PATH not set in setup.sh file." && exit 2
		elif [ ! -x $BHOSTS_PATH/bhosts ]; then
			echo "ERROR: Executable $BHOSTS_PATH/bhosts doesn't exist. Check the value of the variable BHOSTS_PATH in setup.sh" && exit 2
		fi
	;;
	pbs) 
		if [ -z $PBSNODES_PATH ]; then 
			echo "ERROR: PBSNODES_PATH not set in setup.sh file." && exit 2
		elif [ ! -x $PBSNODES_PATH/bhosts ]; then
			echo "ERROR: Executable $PBSNODES_PATH/pbsnodes doesn't exist. Check the value of the variable PBSNODES_PATH in setup.sh" && exit 2
		fi
	;;
	*)
		if [ -z $BATCH ]; then
			echo "ERROR: The Batch system is not set. Check the value of the variable BATCH in setup.sh"
			exit 2
		else
			echo "ERROR: $BATCH is a unsupported batch system. Check the value of the variable BATCH in setup.sh"
			exit 2
		fi
	;;
esac

# Get the list of unique hosts in the cluster/site
if [ ! -s $uHosts ] || [ $refresh -eq 1 ]; then
	case $BATCH in 
		condor) 
			$CONDOR_STATUS_PATH/condor_status -l|grep "Machine = \""|grep -v Client|awk '{print $3}'|cut -d\" -f2|sort|uniq > $uHosts
		;;
		lsf) 
			$BHOSTS_PATH/bhosts|awk '{print $1}'|grep -v HOST_NAME|sort|uniq > $uHosts
		;;
		pbs) 
			$PBSNODES_PATH/pbsnodes -a|grep "Host = "|awk '{print $3}'|sort|uniq> $uHosts
		;;
	esac
fi

# Gather individual host information for all hosts in the cluster
if [ ! -s $hostDetails ] || [ $refresh -eq 1 ]; then
	echo -n "Collecting individual host information"
	echo host\~processors\~cores\~model\~mhz > $hostDetails
	cpufile=/proc/cpuinfo
	for host in `cat $uHosts`
	do
		echo -n .
		np=`ssh $host cat $cpufile|grep processor|wc -l` 			# number of processors
		nc=`ssh $host cat $cpufile|grep "cpu cores"|head -1|awk '{print $4}'` 	# number of cores
		model=`ssh $host cat $cpufile|grep "model name"|head -1|cut -d: -f2` 	# cpu model
		mhz=`ssh $host cat $cpufile|grep "cpu MHz"|head -1` 			# cpu speed
		echo $host\~$np\~$nc\~$model\~$mhz >> $hostDetails 			# Print information to a file 
	done
	echo done
	# summarize the information
	./ci.pl > $hostSummary
	echo "Host information is available at $hostDetails"  
	echo "Host summary is available at $hostSummary"  
	echo
else
	echo "Host information is already available at $hostDetails"  
	echo "Host summary is already available at $hostSummary"  
	echo "use $0 -r to force a refresh of this information"
	echo
fi

# This information could be reported to a central repository as XML data and then imported to a database
###############################################################################################################################
