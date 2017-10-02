var kafka = require('kafka-node');


var tweetLikesAnalyticsListener = module.exports;
var subscribers = [];

tweetLikesAnalyticsListener.subscribeToTweetLikeAnalytics = function( callback) {
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
var TOPIC_NAME = 'Top3TweetLikesPerConference';
var ZOOKEEPER_PORT = 2181;


var consumerOptions = {
    host: EVENT_HUB_PUBLIC_IP + ':' + KAFKA_ZK_SERVER_PORT ,
    groupId: 'consume-tweetLikeAnalytics-for-web-app',
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
    console.log("Message Value "+message.value);
    var likedTweetsCountTop3 = JSON.parse(message.value);
//    {"nrs":[{"tweetId":"1495112906610001DCWw","conference":"oow17","count":27,"window":null},{"tweetId":"1496421364049001nhas","conference":"oow17","count":19,"window":null},{"tweetId":"1497393952355001DjRn","conference":"oow17","count":18,"window":null},null]}
    subscribers.forEach( (subscriber) => {
        subscriber(JSON.stringify(likedTweetsCountTop3));
        
    })
  }
  
  process.once('SIGINT', function () {
    async.each([consumerGroup], function (consumer, callback) {
      consumer.close(true, callback);
    });
  });
