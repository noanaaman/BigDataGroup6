#!/bin/bash

# grep -m 1

while read -r line || [[ -n "$line" ]]; do
  # complete some tasks with $line
  # get the article title for matching against the professions file
  title = sed -e 's/\(.*\)<.*/\1/' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' $line

  ## match title against professions file
  #profs = grep -m 1 $title professions.txt
  ## cut off the title
  #profs = sed -e 's/.*:\(.*\)/\1/' $profs
  ## if this article isn't in the profs list at all, skip to the next one!
  #if [[ -z profs ]]; then
  #  continue;
  #fi

  # store the lemmas and indices too
  wcs = sed -e 's/.*\(.*\)/\1/' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' $line
  # clear out sequence variable for current instance
  seq = ""
  # continue until wcs is emptied
  while [ -n "$wcs" ]; do
    # current lemma and count
    lemma = sed -e 's/<\(.*\),.*/\1/' $wcs
    count = sed -e 's/.*,\([0-9]*\)>/\1/' $wcs
    # update wcs
    wcs = sed -e 's/.*>,\(.*\)/\1/' $wcs 
    # repeat the lemma x count
    for i in { 1..$count } do
      # add this series of strings to the current sequence
      seq=$seq$lemma" "  
  done
  
  # write the title and sequence to file
  echo -e "$title\t$seq" >> sequences.txt

done
