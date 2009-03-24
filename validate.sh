#! /bin/bash

config_file=$1
echo "Using $config_file"

function find_self_dir()
{
    MYLOCATION="${BASH_ARGV[0]}"
    export MYLOCATION="${MYLOCATION%/*}"
    if [ $MYLOCATION == "test_find_self.sh" ] ; then
        export MYLOCATION=`pwd`
    fi
    if [[ $MYLOCATION != /* ]] ; then
        export MYLOCATION=`pwd`/$MYLOCATION
    fi
    MYLOCATION=$(readlink -f $MYLOCATION)

    echo $MYLOCATION
}
export VDT_LOCATION=$(find_self_dir)
export GIP_LOCATION=$VDT_LOCATION/gip
echo "VDT_LOCATION=$VDT_LOCATION"
echo "GIP_LOCATION=$GIP_LOCATION"

python $GIP_LOCATION/bin/TestRunner.py $config_file
