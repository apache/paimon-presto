# Apache Paimon Presto Connector

## 说明
我的环境
```shell
mvn clean install -DskipTests -Dgpg.skip -Drat.skip -Papache-release -am -pl paimon-presto-0.273 -Dpresto.version=0.287
# -Dhadoop.apache2.version=3.3.4
```

单编辑一下个模块
```shell
mvn install -Dgpg.skip -Drat.skip -DskipTests -Papache-release -pl paimon-presto-common
```
> 如果要让hive catalog查询paimon表，则需要将出的`paimon-presto-common`包中的`META-INFO/services/com.facebook.presto.spi.Plugin`文件删除掉














This repository is Presto Connector for the [Apache Paimon](https://paimon.apache.org/) project.

## About

Apache Paimon is an open source project of [The Apache Software Foundation](https://apache.org/) (ASF).

## Getting Started

### Build

```bash
mvn clean install -DskipTests
```

During the packaging process, you may encounter the following errors: 

```
[ERROR] Failed to execute goal on project paimon-presto: Could not resolve dependencies for project org.apache.paimon:paimon-presto:pom:0.7-SNAPSHOT: The following artifacts could not be resolved: org.apache.paimon:paimon-bundle:jar:0.7-SNAPSHOT (absent): Could not find artifact org.apache.paimon:paimon-bundle:jar:0.7-SNAPSHOT in xxx
```

You can resolve the packaging issue by adding the following Maven repository addresses to your `settings.xml` or to the `pom.xml` of the current project: 

```xml
<repositories>
    <repository>
        <id>apache-releases</id>
        <name>apache releases</name>
        <url>https://repository.apache.org/content/repositories/releases/</url>
    </repository>
    <repository>
        <id>apache-snapshots</id>
        <name>apache snapshots</name>
        <url>https://repository.apache.org/content/repositories/snapshots/</url>
    </repository>
</repositories>
```

After the packaging is complete, you can choose the corresponding connector based on your own Presto version: 

| Version         | Package                                                                       |
|-----------------|-------------------------------------------------------------------------------|
| [0.236, 0.268)  | `./paimon-presto-0.236/target/paimon-presto-0.236-0.7-SNAPSHOT-plugin.tar.gz` |
| [0.268, 0.273)  | `./paimon-presto-0.268/target/paimon-presto-0.268-0.7-SNAPSHOT-plugin.tar.gz` |
| [0.273, latest] | `./paimon-presto-0.273/target/paimon-presto-0.273-0.7-SNAPSHOT-plugin.tar.gz` |

Of course, we also support different versions of Hive and Hadoop. But note that we utilize 
Presto-shaded versions of Hive and Hadoop packages to address dependency conflicts. 
You can check the following two links to select the appropriate versions of Hive and Hadoop:

[hadoop-apache2](https://mvnrepository.com/artifact/com.facebook.presto.hadoop/hadoop-apache2)

[hive-apache](https://mvnrepository.com/artifact/com.facebook.presto.hive/hive-apache)

Both Hive 2 and 3, as well as Hadoop 2 and 3, are supported.

For example, if your presto version is 0.274, hive and hadoop version is 2.x, you could run:

```bash
mvn clean install -DskipTests -am -pl paimon-presto-0.273 -Dpresto.version=0.274 -Dhadoop.apache2.version=2.7.4-9 -Dhive.apache.version=1.2.2-2
```

### Install Paimon Connector

```bash
tar -zxf paimon-presto-${PRESTO_VERSION}/target/paimon-presto-${PRESTO_VERSION}-${PAIMON_VERSION}-plugin.tar.gz -C ${PRESTO_HOME}/plugin
```

Note that, the variable `PRESTO_VERSION` is module name, must be one of 0.236, 0.268, 0.273.

### Configuration

```bash
cd ${PRESTO_HOME}
mkdir -p etc/catalog
```

**Query FileSystem table:**

```bash
vim etc/catalog/paimon.properties
```

and set the following config:

```properties
connector.name=paimon
# set your filesystem path, such as hdfs://namenode01:8020/path and s3://${YOUR_S3_BUCKET}/path
warehouse=${YOUR_FS_PATH}

# Enable paimon query pushdown.
paimon.query-pushdown-enabled=true
```

If you are using HDFS FileSystem, you will also need to do one more thing: choose one of the following ways to configure your HDFS:

- set environment variable HADOOP_HOME.
- set environment variable HADOOP_CONF_DIR.
- configure `hadoop-conf-dir` in the properties.

If you are using S3 FileSystem, you need to add `paimon-s3-${PAIMON_VERSION}.jar` in `${PRESTO_HOME}/plugin/paimon` and additionally configure the following properties in `paimon.properties`:

```properties
s3.endpoint=${YOUR_ENDPOINTS}
s3.access-key=${YOUR_AK}
s3.secret-key=${YOUR_SK}
```

**Query HiveCatalog table:**

```bash
vim etc/catalog/paimon.properties
```

and set the following config:

```properties
connector.name=paimon
# set your filesystem path, such as hdfs://namenode01:8020/path and s3://${YOUR_S3_BUCKET}/path
warehouse=${YOUR_FS_PATH}
metastore=hive
uri=thrift://${YOUR_HIVE_METASTORE}:9083
```




