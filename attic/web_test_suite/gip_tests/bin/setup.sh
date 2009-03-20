#! /bin/bash

for i in /etc/profile.d/*.sh; do
    if [ -r "$i" ]; then
        . $i
    fi
done
unset i

# Change $BASE_PATH to point to the location that you put gip_tests
export BASE_PATH=gip_tests
export PYTHONPATH=$PYTHONPATH:$BASE_PATH:$BASE_PATH/bin:$BASE_PATH/lib:$BASE_PATH/libexec
