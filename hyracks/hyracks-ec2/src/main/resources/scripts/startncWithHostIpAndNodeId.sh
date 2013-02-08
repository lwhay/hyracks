#!/bin/bash

CUR_DIR=$(cd $(dirname "$0"); pwd)
HYRACKS_EC2_APPASSEMBLER=$(cd $(dirname "$CUR_DIR"); pwd)

hostname

#Import cluster properties
. $HYRACKS_EC2_APPASSEMBLER/conf/cluster.properties

if test -z "$NCTMP_DIR"
then
	echo "Can't load cluster.properties"
	exit
fi

#Get the IP address of the cc
CCHOST=$1
IPADDR=$2

#Get node ID
NODEID=$3

if test -z "$CCHOST"
then
	echo "no parameter"
	exit
fi

#Clean up temp dir

rm -rf $NCTMP_DIR
mkdir $NCTMP_DIR

#Clean up log dir
rm -rf $NCLOGS_DIR
mkdir $NCLOGS_DIR


#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS | tr "," "\n")
for io_dir in $io_dirs
do
	rm -rf $io_dir
	mkdir $io_dir
done

#Set JAVA_HOME
export JAVA_HOME=$JAVA_HOME


#Set JAVA_OPTS
export JAVA_OPTS=$NCJAVA_OPTS

#Enter the temp dir
cd $NCTMP_DIR

#Launch hyracks nc
$CUR_DIR/hyracksnc -cc-host $CCHOST -cc-port $CC_CLUSTERPORT -cluster-net-ip-address $IPADDR  -data-ip-address $IPADDR -node-id $NODEID -iodevices "${IO_DIRS}" &> $NCLOGS_DIR/$NODEID.log &
