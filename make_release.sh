#!/bin/bash

set -o noglob
usage='usage: make_release.sh releasename'

if [ -z "$GIP_LOCATION" ] ; then
    echo 'please set GIP_LOCATION before proceeding'
    exit 1
fi

tagdir="${GIP_LOCATION}/tags"

if [ -z "$1" ] ; then
    echo $usage
    echo "Available releases are: "
    echo
    ls $tagdir
    exit 2
fi

releasename=$1

if [ ! -d "$tagdir/$releasename" ] ; then
    echo "Can't find $releasename.  Choices are:"
    echo
    ls $tagdir
    exit 3
fi

excludelist="--exclude */.svn --exclude *~ --exclude #*# --exclude *.tar.gz --exclude *.tgz --exclude changelog"

if [ -z "$GIP_RELEASE_LOCATION" ] ; then
    export GIP_RELEASE_LOCATION=/tmp
fi

tar cfvz ${GIP_RELEASE_LOCATION}/$releasename.tgz -C $tagdir $releasename $excludelist
