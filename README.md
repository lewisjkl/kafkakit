# KafkaKit
*The Kafka CLI you've always wanted.*

## Installation

```bash
coursier bootstrap -r https://packages.confluent.io/maven/ com.lewisjkl:kafkakit_2.13:0.0.5 -o kafkakit
```

## Configuration

Currently, their is no way to build your configuration file through the CLI itself, so for now you just have to create a json file at `~/.kafkakit.json`. The file needs to be of the format:

```json
{
  "kafkaClusters" : [
    {
      "nickname" : "local",
      "bootstrapServers" : "localhost:9092",
      "defaultKeyFormat" : "Avro",
      "defaultValueFormat" : "Avro",
      "schemaRegistryUrl" : "http://localhost:8081"
    }
  ],
  "defaultClusterNickname" : "local"
}
```

You must specify at least one Kafka cluster in your configuration. The cluster that you provide as `defaultClusterNickname` will be the cluster that is automatically selected if you don't specify a different cluster when running a command. Currently supported deserialization formats are `Avro` and `String`. If you are not using Avro, you don't need to specify the `schemaRegistryUrl`.

## Credits

Thank you to @kubukoz since I relied on his [spotify-next](https://github.com/kubukoz/spotify-next) repo when creating this project.
