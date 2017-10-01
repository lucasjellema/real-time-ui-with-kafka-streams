var Twit = require('twit');
const kafka = require('kafka-node');
const express = require('express');
const app = express();

const { twitterconfig } = require('./twitterconfig');

// tru event hub var EVENT_HUB_PUBLIC_IP = '129.144.150.24';
// local Kafka Cluster
var EVENT_HUB_PUBLIC_IP = '192.168.188.102';

// tru event hub var TOPIC_NAME = 'partnercloud17-microEventBus';
var TOPIC_NAME = 'tweetsTopic';
var ZOOKEEPER_PORT = 2181;

var Producer = kafka.Producer;
var client = new kafka.Client(EVENT_HUB_PUBLIC_IP + ':' + ZOOKEEPER_PORT);
var producer = new Producer(client);

let payloads = [
  { topic: TOPIC_NAME, messages: '*', partition: 0 }
];

const bodyParser = require('body-parser');

app.use(bodyParser.json());

var T = new Twit({
  consumer_key: twitterconfig.consumer_key,
  consumer_secret: twitterconfig.consumer_secret,
  access_token: twitterconfig.access_token_key,
  access_token_secret: twitterconfig.access_token_secret,
  timeout_ms: 60 * 1000,
});


var hashtag = "oow17";
var tracks = { track: ['oraclecode', 'javaone', 'oow17'] };
let tweetStream = T.stream('statuses/filter', tracks)
//let j1Stream = T.stream('statuses/filter', { track: "javaone" })
//let genericStream = T.stream('statuses/filter', { track: "oracle" })
tweetstream(tracks, tweetStream);
// tweetstream("javaone", j1Stream);
// tweetstream("oracle", genericStream);

//Environment Parameters
var port = Number(process.env.PORT || 8080);

// Server GET
app.get('/', (req, res) => {
  console.log("Root");
  res.send("Success !");
});

app.get('/stop', (req, res) => {
  console.log("********STOP*******");
  genericStream.stop();
  res.send("Stopped !");
});

app.get('/hashtag/:id', (req, res) => {

  console.log("Old filter: " + hashtag);
  hashtag = req.params.id;
  console.log("New filter request: " + hashtag);
  tweetstream(hashtag, genericStream);
  res.send("Filter Applied: " + hashtag);
});

// server listen
app.listen(port, function () {
  console.log("Listening on " + port);
});

function tweetstream(hashtags, tweetStream) {
  //  tweetStream.stop();
  // tweetStream = T.stream('statuses/filter', { track:   hashtags });
  console.log("Started tweet stream for hashtag #" + JSON.stringify(hashtags));

  tweetStream.on('connected', function (response) {
    console.log("Stream connected to twitter for #" + JSON.stringify(hashtags));
  })
  tweetStream.on('error', function (error) {
    console.log("Error in Stream for #" + JSON.stringify(hashtags) + " " + error);
  })
  tweetStream.on('tweet', function (tweet) {
    console.log(JSON.stringify(tweet));
    console.log(tweet.text);
    // find out which of the original hashtags { track: ['oraclecode', 'javaone', 'oow17'] } in the hashtags for this tweet; 
    //that is the one for the tagFilter property
    // select one other hashtag from tweet.entities.hashtags to set in property hashtag
    var tagFilter;
    var extraHashTag;
    for (var i = 0; i < tweet.entities.hashtags.length; i++) {
      var tag = tweet.entities.hashtags[i].text.toLowerCase();
      console.log("inspect hashtag "+tag);
      var idx = hashtags.track.indexOf(tag);
      if (idx > -1) {
        tagFilter = tag;
      } else {
        extraHashTag = tag
      }
    }//for


    var tweetEvent = {
      "eventType": "tweetEvent"
      , "text": tweet.text
      , "isARetweet": tweet.retweeted_status ? "y" : "n"
      , "author": tweet.user.name
      , "hashtag": extraHashTag
      , "createdAt": tweet.created_at
      , "language": tweet.lang
      , "tweetId": tweet.id
      , "tagFilter": tagFilter
      , "originalTweetId": tweet.retweeted_status ? tweet.retweeted_status.id : null
    };
    KeyedMessage = kafka.KeyedMessage,
      tweetKM = new KeyedMessage(tweetEvent.id, JSON.stringify(tweetEvent)),
      payloads[0].messages = tweetKM;

    producer.send(payloads, function (err, data) {
      if (err) {
        console.error(err);
      }
      console.log(data);
    });


    //  payloads[0].messages = JSON.stringify(tweet);
    //  producer.send(payloads, function (err, data) {
    //  console.log("Sent to EventHub !");});
  });


}
