#!/bin/bash

d="2016-12-03"
while [ "$d" != 2020-01-24 ]; do
    ./import-data.pl >> import.out -d $d -c 2>&1 || exit 1
    d=$(date -I -d "$d + 1 day")
done