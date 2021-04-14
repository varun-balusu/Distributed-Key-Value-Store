#!/bin/bash

declare -a arr=()

for key in {0..150}
do

arr+=( "$(curl -s http://127.0.0.5:8080/get?key=$key)" )


done

declare -p arr
echo "${#arr[@]}"