#!/bin/bash
for filename in perf/*mix*
do
    if [[ -e $filename/parsed.txt && -s $filename/parsed.txt ]]
    then
        echo "exist $filename/parsed.txt"
    else
        echo -n "Parsing $filename..."
        python3 ./analyzer/single_run.py --nogui $filename > $filename/parsed.txt
        echo "Finished"
    fi
done