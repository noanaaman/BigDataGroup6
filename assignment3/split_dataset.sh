#!/bin/bash

# USAGE: bash split_dataset.sh INPUT_FILE OUTPUT_DIR INT
#
# alternatively, call as ./split_dataset.sh INPUT_FILE OUTPUT_DIR INT
# -- but note that this requires permissions adjustments.
# This script will fail if called by sh.
#
# input must be an existing file.
# output must be an existing dir to write outputs to.
# int must be in range (0-100), number of trainset items for each testset item

# check number of args is good
if [ "$#" != 3 ]; then
  echo 'Arg problem: not three but '$#;
  exit 1;
fi
# check first arg is a filepath
if ! [ -f "$1" ]; then
  echo $1' is not a file';
  exit 1;
fi
# check second arg is a directory
if ! [ -d "$2" ]; then
  echo $2' is not a directory';
  exit 1;
fi
# check third arg is a valid int
if [ "$3" -eq 0 ] || ! [ "$3" -eq 0 ]; then
  if [ 1 -gt "$3" ] || [ "$3" -gt 99 ]; then
    echo $3' is not between 0 and 100'
    exit 1;
  fi
  # go ahead and notify the proportions specified
  echo "Specified train:test ratio "$3":1"
  else
    echo $3' is not an integer';
    exit 1;
fi

# set up output files
TEST_PATH=$2"/testset.txt"
TRAIN_PATH=$2"/trainset.txt"

# start an index
i=0
mod=$(($3+1))
while read -r line; do
  if [ $(($i % $mod)) -eq 0 ]; then
    # write one line to a testfile in the specified dir
    echo $line >> $TEST_PATH; else
    #echo -e $i":\ttest\t"$line; else
    #
    # write n lines to a trainfile in the specified dir 
    echo $line >> $TRAIN_PATH;
    #echo -e $i":\ttrain\t"$line;
  fi
  i=$(($i+1))
done < $1
