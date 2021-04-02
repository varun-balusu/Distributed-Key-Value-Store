#!/bin/bash


# arr=("java" "bread" "nylah" "perry" "bean" "orange" "rfeds" "four" "candy" "cotton" "rfid" "rfed" "rent")

# for key in "${arr[@]}"
# do
# curl 'http://localhost:8080/set?key='$key'&value=good'

# done

for key in {1..100}
do
curl 'http://localhost:8080/set?key='$key'&value=good'

done
