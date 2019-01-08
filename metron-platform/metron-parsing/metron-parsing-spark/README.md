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

