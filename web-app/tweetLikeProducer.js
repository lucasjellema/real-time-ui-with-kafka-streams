var kafka = require('kafka-node');


var tweetLikeProducer = module.exports;

// tru event hub var EVENT_HUB_PUBLIC_IP = '129.144.150.24';
// local Kafka Cluster
var EVENT_HUB_PUBLIC_IP = '192.168.188.102';

// tru event hub var TOPIC_NAME = 'partnercloud17-microEventBus';
var TOPIC_NAME = 'tweetLikes';
var ZOOKEEPER_PORT = 2181;

var Producer = kafka.Producer;
var client = new kafka.Client(EVENT_HUB_PUBLIC_IP + ':' + ZOOKEEPER_PORT);
var producer = new Producer(client);

let payloads = [
  { topic: TOPIC_NAME, messages: '*', partition: 0 }
];

tweetLikeProducer.produceTweetLike = function(tweetLikeEvent) {
    KeyedMessage = kafka.KeyedMessage,
      tweetKM = new KeyedMessage(tweetLikeEvent.tweetId, JSON.stringify(tweetLikeEvent)),
      payloads[0].messages = tweetKM;

    producer.send(payloads, function (err, data) {
      if (err) {
        console.error(err);
      }
      console.log(data);
    });
}//produceTweetLike
