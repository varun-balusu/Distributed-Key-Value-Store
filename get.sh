#!/bin/bash

declare -a arr=()

for key in {1..100}
do

arr+=( "$(curl -s http://127.0.0.3:8080/get?key=$key)" )


done