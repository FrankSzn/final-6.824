#!/bin/bash
set -e 
if [[ $# -ne 2 ]]; then
	echo "Usage: $0 [test] [repeat time]"
	exit 1
fi

for (( i = 0; i < $2; i++ )); do
	(time go test -race -run $1 ) >> result1.txt
	sleep 10
done