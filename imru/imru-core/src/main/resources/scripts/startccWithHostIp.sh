#!/bin/bash

CUR_DIR=$(cd $(dirname "$0"); pwd)
HYRACKS_EC2_APPASSEMBLER=$(cd $(dirname "$CUR_DIR"); pwd)

hostname

#Import cluster properties
. $HYRACKS_EC2_APPASSEMBLER/conf/cluster.properties

#Get the IP address of the cc
CCHOST=$1

if test -z $CCTMP_DIR
then
	echo "Can't load cluster.properties"
	exit
fi

#Remove the temp dir
rm -rf $CCTMP_DIR
mkdir $CCTMP_DIR

#Remove the logs dir
rm -rf $CCLOGS_DIR
mkdir $CCLOGS_DIR

#Export JAVA_HOME and JAVA_OPTS
export JAVA_HOME=$JAVA_HOME
export JAVA_OPTS=$CCJAVA_OPTS

#Launch hyracks cc script
chmod u+x $CUR_DIR/hyrackscc
$CUR_DIR/hyrackscc -client-net-ip-address $CCHOST -cluster-net-ip-address $CCHOST -client-net-port $CC_CLIENTPORT -cluster-net-port $CC_CLUSTERPORT -max-heartbeat-lapse-periods 999999 -default-max-job-attempts 0 -job-history-size 3 &> $CCLOGS_DIR/cc.log &
