#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
set -x

rm -rf output
rm -rf spark*.tgz

latest_commit=$(git log -1 --pretty=oneline | cut -d ' ' -f 1)

if test -z "$BUILD_VERSION"
then
	  scm_version_id=$latest_commit
 else
	  scm_version_id=$BUILD_VERSION
fi

export MAVEN_OPTS="-Xmx8g -XX:ReservedCodeCacheSize=2g"

# Build and generate output
dev/make-distribution.sh --name zhenchao --tgz -Pyarn -Phadoop-3.2 -Phive -Phive-thriftserver -Pkubernetes -Dprotobuf.version=2.5.0 $@
tar -zxf spark-*-zhenchao.tgz
mv spark-*-zhenchao output

# Generate conf
rm -rf output/conf
cp -a conf output

cp -r sbin output

# Add streaming jar
cp `ls external/kafka-0-10-sql/target/*.jar | grep -v original | grep -v test | grep -v source` output/jars/spark-sql-kafka-0-10.jar
cp `ls external/kafka-0-10-assembly/target/*.jar | grep -v original | grep -v test | grep -v source` output/jars/spark-streaming-kafka-0-10-assembly.jar

# add avro-related to output/jars
cp `ls external/avro/target/*.jar | grep -v original | grep -v test | grep -v source`  output/jars

