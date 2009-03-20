#! /bin/bash

# Change $BASE_PATH to point to the location that you put gip_tests
export BASE_PATH=gip_tests
source $BASE_PATH/bin/setup.sh
python $BASE_PATH/bin/TestRunner.py $BASE_PATH
