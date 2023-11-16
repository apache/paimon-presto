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

You can also set the version of presto and hive:

```
-Dpresto.version=${YOUR_PRESTO_VERSION} -Dhive.version=${YOUR_HIVE_VERSION}
```

For example, if your presto version is 0.274 and hive version is 2.3.4, you could run:

```
mvn clean install -DskipTests -am -pl paimon-presto-0.273 -Dpresto.version=0.274 -Dhive.version=2.3.4
```

### Install

```
tar -zxf paimon-presto-${PRESTO_VERSION}/target/paimon-presto-${PRESTO_VERSION}-${PAIMON_VERSION}-plugin.tar.gz -C paimon-presto-${PRESTO_VERSION}/target
cp -r paimon-presto-${PRESTO_VERSION}/target/paimon-presto-${PRESTO_VERSION}-${PAIMON_VERSION}/paimon ${PRESTO_HOME}/plugin
```

Note that, the variable `PRESTO_VERSION` is module name, must be one of 0.236, 0.268, 0.273.



