#!/bin/bash

declare -a arr=()

for key in {1..100}
do

arr+=( "$(curl -s http://127.0.0.1:8080/delete?key=$key)" )


done

declare -p arr
echo "${#arr[@]}"