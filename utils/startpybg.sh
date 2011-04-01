#!/bin/bash
#
# startpybg: START PYthon in BackGround
#
# startpybg FNAME [args...]
#
# A bash script to launch a python script as a background process.
# 
# FNAME: the file name of the python script to be executed in the 
# background.
#
# Writes the following file:
#     FNAME.pybg    lists the start time, PID, stdout, and stderr.

fout="$1.pybg"
echo -e "started: "`date` > $fout
python $* >> $fout 2>&1 &

echo -e "pid:     " $!"\n" >> $fout 

