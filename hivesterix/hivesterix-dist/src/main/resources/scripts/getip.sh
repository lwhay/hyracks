#!/bin/bash
#/*
# Copyright 2009-2013 by The Regents of the University of California
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# you may obtain a copy of the License from
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*/

#get the OS
OS_NAME=`uname -a|awk '{print $1}'`
LINUX_OS='Linux'

INTERFACES="en0 en1 en2 en3 en4 lo0"

if [ $OS_NAME = $LINUX_OS ];
then
  INTERFACES="eth0 em1 lo"
fi

for INTERFACE in $INTERFACES
do
  [ "$IPADDR" = "" ] && {
    IPADDR=`/sbin/ifconfig $INTERFACE | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
  }
done
echo $IPADDR
