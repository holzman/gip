#!/bin/bash

set -o noglob
usage='usage: make_release.sh releasename'
tag_regex="*1.3*"
if [ -z "$GIP_SRC_LOCATION" ] ; then
    echo 'please set GIP_SRC_LOCATION before proceeding'
    exit 1
fi

cd $GIP_SRC_LOCATION

if [ -z "$1" ] ; then
    echo $usage
    echo "Available releases are: "
    echo
    git tag -l $tag_regex
    exit 2
fi

releasename=$1

git show-ref --verify refs/tags/$releasename >& /dev/null
if [ $? -ne 0 ]; then
    echo "Can't find $releasename.  Choices are:"
    echo
    git tag -l $tag_regex
    exit 3
fi

if [ -z "$GIP_RELEASE_LOCATION" ] ; then
    export GIP_RELEASE_LOCATION=/tmp
fi

tarball=${GIP_RELEASE_LOCATION}/${releasename}.tar
git archive --format=tar --prefix=$releasename/ $releasename  > ${tarball}

# add release tag into tarball
tmpdir=`mktemp -d`
cd $tmpdir
mkdir -p ${releasename}/gip/etc
echo $releasename > ${releasename}/gip/etc/gip_release.txt
tar -r -f ${tarball} ${releasename}/gip/etc/gip_release.txt
rm -f ${releasename}/gip/etc/gip_release.txt
cd $GIP_SRC_LOCATION
rm -rf $tmpdir

gzip -f ${tarball}
mv ${tarball}.gz ${GIP_RELEASE_LOCATION}/${releasename}.tgz


