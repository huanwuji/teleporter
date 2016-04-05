#!/bin/bash
if [ -z "$1" ]
then
        ps aux | grep [j]ava | grep teleporter.web.Main | awk '{print $2}' | xargs -n 1 kill
else
        ps aux | grep [j]ava | grep teleporter.web.Main | grep $1 | awk '{print $2}' | xargs -n 1 kill
fi