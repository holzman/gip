#!/bin/bash

function usage
{
cat << EOF
usage: $0 options

This script will run the GIP unittests.  By default this will run test_gip.py.
Note that the -V option is required.

OPTIONS:
   -h      Show this message
   -V      Set VDT_LOCATION (required)
   -l      Set LOG_LOCATION (optional - defaults to /tmp/gip_unittest_logs)
   -t      The test to run (optional - defaults to test_gip.py)
EOF
}

function check_args
{
	while getopts “ht:V:l:” OPTION
	do
        case $OPTION in
             h)
				usage
				exit 1
				;;
             t)
				export UNITTEST=$OPTARG
				;;
             V)
                export VDT_LOCATION=$OPTARG
                export GIP_LOCATION=$VDT_LOCATION/gip
                ;;
             l)
                export LOG_LOCATION=$OPTARG
                ;;
             ?)
                usage
                exit 1
                ;;
        esac
	done
}

check_args $@

export GIP_TESTING=1
if [ -z "$UNITTEST" ] ; then
    export UNITTEST=test_gip.py
fi
if [ -z "$LOG_LOCATION" ] ; then
    export LOG_LOCATION=/tmp/gip_unittest_logs
fi
if [ -z "$VDT_LOCATION" ] ; then
    echo
    echo 'please specify the VDT_LOCATION via the -V option before proceeding'
    echo
    usage
    exit 1
fi
if [ -z "$GIP_LOCATION" ] ; then
    echo
    echo 'please specify the VDT_LOCATION via the -V option before proceeding'
    echo
    usage
    exit 1
fi
if [[ -d $LOG_LOCATION ]]; then
    echo "$LOG_LOCATION exists"
else
    echo
    echo "mkdir -p $LOG_LOCATION"
    echo
    mkdir -p $LOG_LOCATION
fi

echo "VDT_LOCATION:     $VDT_LOCATION"
echo "GIP_LOCATION:     $GIP_LOCATION"
echo "Log file:         $LOG_LOCATION/gip_unittest.log"
echo "Running Unittest: $UNITTEST"

original_dir=`pwd`
cd $VDT_LOCATION/test
python $UNITTEST >> $LOG_LOCATION/gip_unittest.log 2>&1
UNITTEST_RETURN_CODE=$?
cd $original_dir

mv $GIP_LOCATION/var/logs/* $LOG_LOCATION 

echo "Return Code:      $UNITTEST_RETURN_CODE"
echo
echo "If the return code is non zero, at least one unittest failed. Check"
echo "$LOG_LOCATION/gip_unittest.log for any error messages."

