// before running, either globally install kafka-node  (npm install kafka-node)
// or add kafka-node to the dependencies of the local application
var fs = require('fs')
// local Kafka Cluster
var EVENT_HUB_PUBLIC_IP = '192.168.188.102';

var TOPIC_NAME = 'tweetsTopic';
var ZOOKEEPER_PORT = 2181;

var kafka = require('kafka-node');
var Producer = kafka.Producer;
var client = new kafka.Client(EVENT_HUB_PUBLIC_IP + ':'+ZOOKEEPER_PORT);
var producer = new Producer(client);


let payloads = [
    { topic: TOPIC_NAME, messages: '*', partition: 0 }
  ];
  

producer.on('ready', function () {
    console.log("producer  is ready");
    produceTweetMessage();
    producer.send(payloads, function (err, data) {
        console.log("send is complete " + data);
        console.log("error " + err);
    });
});

var averageDelay = 13500;  // in miliseconds
var spreadInDelay = 500; // in miliseconds

var oowSessions = JSON.parse(fs.readFileSync('oow2017-sessions-catalog.json', 'utf8'));
var j1Sessions = JSON.parse(fs.readFileSync('javaone2017-sessions-catalog.json', 'utf8'));

console.log(oowSessions);

var prefixes = ["Interesting session","Come attend cool stuff","Enjoy insights ","See me present","Hey mum, I am a speaker  "];

function produceTweetMessage(param) {
    var oow = Math.random() < 0.7;    
    var prefix = prefixes[Math.floor(Math.random() *(prefixes.length))] ;    
    var sessions = oow?oowSessions:j1Sessions;
    var sessionSeq = sessions?Math.floor((Math.random()*sessions.length )):1;
    var session =sessions[sessionSeq];
    if (!session.participants || !session.participants[0]) return;
    var tweetEvent = {
        "eventType": "tweetEvent"
        , "text": `${prefix} at #${oow?'oow17':'javaone'} ${session.title}`
        , "isARetweet": 'N'
        , "author":  `${session.participants[0].firstName} ${session.participants[0].lastName}`
        , "hashtag": sessions[sessionSeq].code
        , "createdAt": null
        , "language": "en"
        , "tweetId": session.sessionID
        , "tagFilter": oow?'oow17':'javaone'
        , "originalTweetId": null
      };
      KeyedMessage  = kafka.KeyedMessage,
      tweetKM = new KeyedMessage(tweetEvent.id, JSON.stringify(tweetEvent)),
      payloads[0].messages = tweetKM;
    
    producer.send(payloads, function (err, data) {
        if (err) {
          console.error(err);
        }
        console.log(data);
      });

    var delay = averageDelay + (Math.random() -0.5) * spreadInDelay;
    //note: use bind to pass in the value for the input parameter currentCountry     
    setTimeout(produceTweetMessage.bind(null, 'somevalue'), delay);             

}

producer.on('error', function (err) {
    console.error("Error "+err);
 })