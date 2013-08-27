#!/bin/bash
set -e
set -o pipefail

GENOMIX_HOME="$( dirname "$( cd "$(dirname "$0")" ; pwd -P )" )"  # script's parent dir's parent
cd "$GENOMIX_HOME"

if [ -e "$GENOMIX_HOME"/conf/cc.pid ] ; then
  echo "existing CC pid found... Not starting a new CC!"
  exit 1
fi
  
#Import cluster properties
. conf/cluster.properties
#Get the IP address of the cc
CCHOST_NAME=`cat conf/master`
CCHOST=`bin/getip.sh`

#Remove the temp dir
#rm -rf $CCTMP_DIR
mkdir -p $CCTMP_DIR

#Remove the logs dir
#rm -rf $CCLOGS_DIR
mkdir -p $CCLOGS_DIR

#Export JAVA_HOME and JAVA_OPTS
export JAVA_HOME=$JAVA_HOME
export JAVA_OPTS=$CCJAVA_OPTS

cd $CCTMP_DIR
#Launch hyracks cc script
if [ -f "${GENOMIX_HOME}/conf/topology.xml"  ]; then
#Launch hyracks cc script with topology
"${GENOMIX_HOME}"/bin/genomixcc -client-net-ip-address $CCHOST -cluster-net-ip-address $CCHOST -client-net-port $CC_CLIENTPORT -cluster-net-port $CC_CLUSTERPORT -max-heartbeat-lapse-periods 999999 -default-max-job-attempts 0 -job-history-size 0 -cluster-topology "${GENOMIX_HOME}/conf/topology.xml" &> "$CCLOGS_DIR"/cc.log &
else
#Launch hyracks cc script without toplogy
"${GENOMIX_HOME}"/bin/genomixcc -client-net-ip-address $CCHOST -cluster-net-ip-address $CCHOST -client-net-port $CC_CLIENTPORT -cluster-net-port $CC_CLUSTERPORT -max-heartbeat-lapse-periods 999999 -default-max-job-attempts 0 -job-history-size 0 &> "$CCLOGS_DIR"/cc.log &
fi

PID=$!
echo "master: "`hostname`$'\t'$PID
echo $PID > "$GENOMIX_HOME"/conf/cc.pid
