#!/bin/bash
# this mini program is used to stream data from the findata director to nsecmd topic
# the arguments should be explanatory
dir2Read=$1
yname=$2
mname=$3
streamrate=$4
sleeprate=$5
echo $dir2Read
for x in $dir2Read/*$mname*$yname*.csv
do
	echo $x  
	# we hhere find the total number of lines in the file
	y=`wc -l $x | cut -d ' ' -f 1`
	# we leave the header out
	idx=2
	while [ $idx -le $y ]
	do
		#echo "sed -n $idx,$(( $idx + 100 ))"
		# here we cut out the traling comma at the end and print a key value pair where
		# the first field is the stock name and $0 is the full line
		sed -n $idx,$(( $idx + $streamrate ))p $x | sed  s/,$//g | awk -F ',' '{ print $1 ":" $0 }' | \
		kafka-console-producer --broker-list localhost:9092 --topic nsecmd --property parse.key=true --property key.separator=:
	sleep $sleeprate
	idx=$(( $idx + $streamrate )) 
	done
done

