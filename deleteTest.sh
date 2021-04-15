#!/bin/bash

declare -a arr=()
declare -a result=()

for key in {0..2000}
do

arr+=( "$(curl -s http://127.0.0.3:8080/get?key=$key)" )


done

# read values loop
for i in {0..2000}
do
if [ ! -z ${arr[$i]} ]

then
    result[${#result[@]}]=${arr[$i]}
fi


done

declare -p arr
echo "${#arr[@]}"

if [ ${#result[@]} -eq 0 ]

then
    echo 'TEST PASSED'
else 
    echo 'TEST FAILED'
fi
# echo ${#result[@]}