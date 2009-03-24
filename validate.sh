#! /bin/bash

usage()
{
cat << EOF
usage: validate.sh options

This script launch the GIP Validator with the appropriate environment.
If this script is passed "-c config_file", the GIP Validator will use
that config file otherwise it will assume $GIP_LOCATION/etc/gip_tests.conf
is the config file to use.

OPTIONS:
   -h      Show this message
   -c      a custom configuration file
EOF
}

while getopts “hc:” OPTION
do
     case $OPTION in
         h)
             usage
             exit 1
             ;;
         c)
             config_file=$OPTARG
             ;;
         ?)
             usage
             exit
             ;;
     esac
done

declare -x RUNDIRECTORY="${0%/*}"
declare -x SCRIPTNAME="${0##*/}"

export VDT_LOCATION=`readlink -f $RUNDIRECTORY`
export GIP_LOCATION=$VDT_LOCATION/gip
echo "VDT_LOCATION=$VDT_LOCATION"
echo "GIP_LOCATION=$GIP_LOCATION"
echo
echo "Using configuration file:"
echo "   $config_file"
echo
echo "executing:"
echo "   python $GIP_LOCATION/bin/TestRunner.py $config_file"
echo
$GIP_LOCATION/bin/TestRunner.py -c $config_file
