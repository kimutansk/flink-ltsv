# flink-ltsv

[Ltsv(Labeled Tab-separated Values)](http://ltsv.org/) format for [Apache Flink](https://flink.apache.org/).

## Dependency

```
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.kimutansk" % "flink-ltsv" % "0.1.0-flink1.6.0"
```

## Configuration

| Name | Desc | Type | Default | Note |
|:-----|:-----|:-----|:--------|:-----|
| format.derive-schema | Whether drived from table schema or not. | Booelan | false | |
| format.schema | Serializer/Deserializer format schema. | String | N/A(required)| Please see [Type Strings](https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/table/connect.html#type-strings)'s `named row`. |
| format.timestamp-format | Serializer/Deserializer timestamp format. | String | `yyyy-MM-dd'T'HH:mm:ssXXX` | |
| format.fail-on-missing-field | In deserialization, whether occurs error or not when the field does not exist. | boolean | false | |

## Usage example

flink-ltsv adopts mainly SQL Client, declaratively via YAML configuration files.  
So programmatically usage is now experimental.

### declaratively

```
format:
  type: ltsv
  schema: "ROW(lon FLOAT, rideTime TIMESTAMP)"
```

### programmatically

```
.withFormat(Ltsv().schema("ROW(lon FLOAT, rideTime TIMESTAMP)"))
```

### License

- License: Apache License, Version 2.0
