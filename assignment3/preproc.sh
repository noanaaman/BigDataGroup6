#!/bin/bash

while read -r line || [ -n "$line" ]; do
  # complete some tasks with $line
  # get the article title for matching against the professions file
  title=$(echo $line | sed -e 's/\([^<]*\)<.*/\1/' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')

  ## match title against professions file
  #profs = grep -m 1 $title professions.txt
  ## cut off the title
  #profs = sed -e 's/.*:\(.*\)/\1/' $profs
  ## if this article isn't in the profs list at all, skip to the next one!
  #if [[ -z profs ]]; then
  #  continue;
  #fi

  # store the lemmas and indices: grab everything after the title
  wcs=$(echo $line | sed -e 's/[^<]*\(.*\)/\1/' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  # clear out sequence variable for current instance
  seq=""

  # continue until no more lemma counts remain
  while [ -n "$wcs" ]; do

    # current lemma and count:
    # lemma: everything between open bracket and comma
    lemma=$(echo $wcs | sed -e 's/< *\([^,]*\).*/\1/')
    # count: digits between comma and close bracket
    count=$(echo $wcs | sed -e 's/[^,]*,\([0-9]*\)>.*/\1/')
    # update wcs: cut off everything before first 
    wcs=$(echo $wcs | sed -e 's/[^>]*>,*\(.*\)/\1/')

    #echo $lemma
    #echo $count
    #echo

    # repeat the lemma count times in the current title's sequence
    for i in $(seq 1 $count); do
      # add this series of strings to the current sequence
      seq=$seq$lemma" "  
      #echo "repeating $lemma $count times; currently at $i"
    done
    #echo $seq
  done
  #echo $seq
  
  # write the title and sequence to file
  echo $title"\t"$seq >> sequences.txt

##############################################################
# ADJUST THIS PATH TO CHANGE SOURCE OF ARTICLE-LEMMA INDICES #
##############################################################
done <test.txt
