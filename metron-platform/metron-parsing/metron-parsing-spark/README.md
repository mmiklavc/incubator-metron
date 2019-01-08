<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
Spark Parsers

To run on full-dev
1. Spin up full-dev
1. Install Spark2 on full-dev via Ambari.
1. Kill the aggregated parser topology, Metron Parsers, and anything else extraneous
1. `vagrant scp ../../../metron-platform/metron-parsing/metron-parsing-spark/target/metron-parsing-spark-0.7.0-uber.jar /tmp`
1. `vagrant ssh`
1. `sudo su - metron`
1. `export SPARK_MAJOR_VERSION=2`
1.
```
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name bro_test \
  --class org.apache.metron.parsers.ParserApplication \
  /tmp/metron-parsing-spark-0.7.0-uber.jar \
  node1:2181 \
  node1:6667 \
  bro
```

