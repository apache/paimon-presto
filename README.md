# Apache Paimon (incubating) Presto Connector

This repository is Presto Connector for the [Apache Paimon](https://paimon.apache.org/) project.

## About

Apache Paimon is an open source project of [The Apache Software Foundation](https://apache.org/) (ASF).

## Getting Started

### Build

| Version         | Command                                                     |
|-----------------|-------------------------------------------------------------|
| [0.236, 0.268)  | `mvn clean install -DskipTests -am -pl paimon-presto-0.236` |
| [0.268, 0.273)  | `mvn clean install -DskipTests -am -pl paimon-presto-0.268` |
| [0.273, latest] | `mvn clean install -DskipTests -am -pl paimon-presto-0.273` |

We utilize Presto-shaded versions of Hive and Hadoop packages to address dependency conflicts. 
You can check the following two links to select the appropriate versions of Hive and Hadoop. 
Both Hive 2 and 3, as well as Hadoop 2 and 3, are supported.

[hadoop-apache2](https://mvnrepository.com/artifact/com.facebook.presto.hadoop/hadoop-apache2)

[hive-apache](https://mvnrepository.com/artifact/com.facebook.presto.hive/hive-apache)

For example, if your presto version is 0.274, hive and hadoop version is 2.x, you could run:

```
mvn clean install -DskipTests -am -pl paimon-presto-0.273 -Dpresto.version=0.274 -Dhadoop.apache2.version=2.7.4-9 -Dhive.apache.version=1.2.2-2
```

### Install Paimon Connector

```
tar -zxf paimon-presto-${PRESTO_VERSION}/target/paimon-presto-${PRESTO_VERSION}-${PAIMON_VERSION}-plugin.tar.gz -C ${PRESTO_HOME}/plugin
```

Note that, the variable `PRESTO_VERSION` is module name, must be one of 0.236, 0.268, 0.273.

### Query

```
cd ${PRESTO_HOME}
mkdir -p etc/catalog
```

Query FileSystem table:

```
vim etc/catalog/paimon.properties
```

and set the following config:

```
connector.name=paimon
warehouse=hdfs://${YOUR_HDFS}/path
```

Query HiveCatalog table:

```
vim etc/catalog/paimon.properties
```

and set the following config:

```
connector.name=paimon
warehouse=hdfs://${YOUR_HDFS}/path
metastore=hive
uri=thrift://${YOUR_HIVE_METASTORE}:9083
```




