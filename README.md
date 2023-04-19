# Apache Paimon (incubating) Presto Connector

This repository is Presto Connector for the [Apache Paimon](https://paimon.apache.org/) project.

## Preparing Paimon Jar File

{{< stable >}}

Download the jar file with corresponding version.

|     Version      | Jar                                                                                                                                                                                                            |
|------------------|-------------------------------------------------------------------------|
| [0.236,0.268)    | [paimon-presto-0.236-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/paimon-{{< version >}}/paimon-presto-0.236-{{< version >}}.jar) |
| [0.268,0.273)    | [paimon-presto-0.268-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/paimon-{{< version >}}/paimon-presto-0.268-{{< version >}}.jar) |
| [0.273,0.279]    | [paimon-presto-0.273-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/paimon-{{< version >}}/paimon-presto-0.273-{{< version >}}.jar) |

{{< /stable >}}

{{< unstable >}}

|     Version      | Jar                                                                                                                                                                                                            |
|------------------|-------------------------------------------------------------------------|
| [0.236,0.268)    | [paimon-presto-0.236-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.236/{{< version >}}/) |
| [0.268,0.273)    | [paimon-presto-0.268-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.268/{{< version >}}/) |
| [0.273,0.279]    | [paimon-presto-0.273-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-presto-0.273/{{< version >}}/) |

{{< /unstable >}}

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.

```
mvn clean install -DskipTests
```

You can find Presto connector jar in `./paimon-presto/paimon-presto-<presto-version>/target/paimon-presto-*.jar`.

Then, copy `paimon-presto-*.jar and flink-shaded-hadoop-*-uber-*.jar` to plugin/paimon.

## Version

Paimon currently supports Presto 0.236 and above.

## Configure Paimon Catalog

Catalogs are registered by creating a catalog properties file in the etc/catalog directory. For example, create etc/catalog/paimon.properties with the following contents to mount the paimon connector as the paimon catalog:

```
connector.name=paimon
warehouse=file:/tmp/warehouse
```

If you are using HDFS, choose one of the following ways to configure your HDFS:

- set environment variable HADOOP_HOME.
- set environment variable HADOOP_CONF_DIR.
- configure fs.hdfs.hadoopconf in the properties.

You can configure kerberos keytag file when using KERBEROS authentication in the properties.

```
security.kerberos.login.principal=hadoop-user
security.kerberos.login.keytab=/etc/presto/hdfs.keytab
```

## Query

```
SELECT * FROM paimon.default.MyTable
```

## About

Apache Paimon is an open source project of [The Apache Software Foundation](https://apache.org/) (ASF).