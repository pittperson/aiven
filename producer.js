const Kafka = require("node-rdkafka");
const { v4: uuidv4 } = require('uuid');

const TOPIC_NAME = "lies-demo";

const producer = new Kafka.Producer({
  'metadata.broker.list': "kafka-ochris-5f65.a.aivencloud.com:16572",
  "security.protocol": "ssl",
  "ssl.key.location": "./ssl/service.key",
  "ssl.certificate.location": "./ssl/service.cert",
  "ssl.ca.location": "./ssl/ca.pem",
  dr_cb: true,
});

function generateTimestamp() {
  const now = new Date();
  return now.toISOString();
}

producer.connect();

const sleep = async (timeInMs) =>
  await new Promise((resolve) => setTimeout(resolve, timeInMs));

const produceMessagesOnSecondIntervals = async () => {
  // produce 500 messages on 1 second intervals
  let i = 0;
  while (i < 50000) {
    const uuid = uuidv4();

    try {
      if (!producer.isConnected()) {
        await sleep(1000);
        continue;
      }
      
      const key = JSON.stringify({uuid: uuid});

      const payload = {
        eventTime: generateTimestamp(),
        eventType: 'temperature_reading',
        temp: Math.random() * 100,
        eventNumber: i
      };
      
      const message = JSON.stringify(payload);

      producer.produce(TOPIC_NAME, null, Buffer.from(message), key);

      console.log(`Message sent: ${message}`);
    } catch (error) {
      console.error("Error sending message: ", error);
    }

    i++;
    await sleep(1000);
  }

  producer.disconnect();
};

produceMessagesOnSecondIntervals();