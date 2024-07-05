#!/bin/sh

work_dir='/data/apps/search/'
cmd=${work_dir}${1}'.py'
out=${work_dir}'logs/'${1}'.out'
echo $cmd $out

nohup $cmd > $out 2>&1 &
