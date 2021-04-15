#!/bin/bash

declare -a arr=()
declare -a result=()

for key in {0..1000}
do

arr+=( "$(curl -s http://127.0.0.2:8080/get?key=$key)" )


done

# read values loop
for i in {0..1000}
do
if [ ${arr[$i]} -ne $i ]

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