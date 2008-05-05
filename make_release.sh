#!/bin/bash 

usage='usage: make_release.sh releasename'

if [ -z "$GIP_LOCATION" ] ; then
    echo 'please set GIP_LOCATION before proceeding'
    exit 1
fi

if [ -z "$1" ] ; then
    echo $usage
    exit 2
fi

releasename=$1
tagdir="${GIP_LOCATION}/tags"

if [ ! -d "$tagdir/$releasename" ] ; then
    echo "Can't find $releasename.  Choices are:"
    echo
    ls $tagdir
    exit 3
fi

excludelist='--exclude */.svn --exclude *~ --exclude #*# --exclude *.tar.gz --exclude *.tgz'

echo "tar cfvz $GIP_LOCATION/$releasename.tgz -C $tagdir $releasename $excludelist"


    
