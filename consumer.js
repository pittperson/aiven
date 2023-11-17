const Kafka = require("node-rdkafka");

const TOPIC_NAME = "lies-demo";

const stream = new Kafka.createReadStream(
  {
    "metadata.broker.list": "kafka-ochris-5f65.a.aivencloud.com:16572",
    "group.id": "GROUP_ID",
    "security.protocol": "ssl",
    "ssl.key.location": "./ssl/service.key",
    "ssl.certificate.location": "./ssl/service.cert",
    "ssl.ca.location": "./ssl/ca.pem",
  },
  { "auto.offset.reset": "beginning" },
  { topics: [TOPIC_NAME] }
);


stream.on("data", (message) => {
  console.log("Got message using SSL:", message.value.toString());
});