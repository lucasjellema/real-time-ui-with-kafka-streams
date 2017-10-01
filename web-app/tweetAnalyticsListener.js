var kafka = require('kafka-node');


var tweetAnalyticsListener = module.exports;
var subscribers = [];

tweetAnalyticsListener.subscribeToTweetAnalytics = function( callback) {
  subscribers.push(callback);
}
//var KAFKA_SERVER_PORT = 6667;
var KAFKA_ZK_SERVER_PORT = 2181;

//var EVENT_HUB_PUBLIC_IP = '129.144.150.24';
//var TOPIC_NAME = 'partnercloud17-microEventBus';
//var ZOOKEEPER_PORT = 2181;
// Docker VM 
// tru event hub var EVENT_HUB_PUBLIC_IP = '129.144.150.24';
// local Kafka Cluster
var EVENT_HUB_PUBLIC_IP = '192.168.188.102';

// tru event hub var TOPIC_NAME = 'partnercloud17-microEventBus';
var TOPIC_NAME = 'tweetAnalyticsTopic';
var ZOOKEEPER_PORT = 2181;


var consumerOptions = {
    host: EVENT_HUB_PUBLIC_IP + ':' + KAFKA_ZK_SERVER_PORT ,
    groupId: 'consume-tweetAnalytics-for-web-app',
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    encoding: 'buffer',
    fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
  };
  
var topics = [TOPIC_NAME];
var consumerGroup = new kafka.ConsumerGroup(Object.assign({id: 'consumer1'}, consumerOptions), topics);
consumerGroup.on('error', onError);
consumerGroup.on('message', onMessage);

function onError (error) {
    console.error(error);
    console.error(error.stack);
  }
  
  function onMessage (message) {
    console.log('%s read msg Topic="%s" Partition=%s Offset=%d', this.client.clientId, message.topic, message.partition, message.offset);
    console.log("Message Key "+message.key);
    var conference =message.key.toString();
    var count = parseInt(message.value.toString('hex'), 16).toString(10);
    subscribers.forEach( (subscriber) => {
        subscriber(JSON.stringify({"eventType":"tweetAnalytics","conference":conference, "tweetCount":count}
      ));
        
    })
  }
  
  process.once('SIGINT', function () {
    async.each([consumerGroup], function (consumer, callback) {
      consumer.close(true, callback);
    });
  });
