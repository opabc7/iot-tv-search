#!/bin/sh

task=$1

case "$task" in
    album|person|virtual_program|heat)
        echo "task:    ${task}"
    ;;
    *)
        echo "$task is not supported"
        exit
esac

work_dir='/data/apps/search/'
cmd=${work_dir}${1}'.py'
out=${work_dir}'logs/'${1}'.out'
echo $cmd $out

nohup $cmd > $out 2>&1 &
