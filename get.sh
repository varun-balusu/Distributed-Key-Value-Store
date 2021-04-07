#!/bin/bash

declare -a arr=()

for key in {1..2000}
do

arr+=( "$(curl -s http://127.0.0.2:8080/get?key=$key)" )


done

declare -p arr
echo "${#arr[@]}"