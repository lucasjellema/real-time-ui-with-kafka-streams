// Handle REST requests (POST and GET) for departments
var express = require('express') //npm install express
  , bodyParser = require('body-parser') // npm install body-parser
  , fs = require('fs')
  , https = require('https')
  , http = require('http')
  , request = require('request');

var logger = require("./logger.js");
var tweetListener = require("./tweetListener.js");
var tweetAnalyticsListener = require("./tweetAnalyticsListener");
var tweetLikesAnalyticsListener = require("./tweetLikesAnalyticsListener");
var tweetLikeProducer = require("./tweetLikeProducer.js");
var sseMW = require('./sse');

const app = express()
  .use(bodyParser.urlencoded({ extended: true }))
  //configure sseMW.sseMiddleware as function to get a stab at incoming requests, in this case by adding a Connection property to the request
  .use(sseMW.sseMiddleware)
  .use(express.static(__dirname + '/public'))
  .get('/updates', function (req, res) {
    console.log("res (should have sseConnection)= " + res.sseConnection);
    var sseConnection = res.sseConnection;
    console.log("sseConnection= ");
    sseConnection.setup();
    sseClients.add(sseConnection);
  });

const server = http.createServer(app);

const WebSocket = require('ws');
// create WebSocket Server
const wss = new WebSocket.Server({ server });
wss.on('connection', (ws) => {
  console.log('WebSocket Client connected');
  ws.on('close', () => console.log('Client disconnected'));

  ws.on('message', function incoming(message) {
    console.log('WSS received: %s', message);
    if (message.indexOf("tweetLike") > -1) {
      var tweetLike = JSON.parse(message);
      var likedTweet = tweetCache[tweetLike.tweetId];
      if (likedTweet) {
        console.log("Liked Tweet: " + likedTweet.text);
        updateWSClients(JSON.stringify({ "eventType": "tweetLiked", "likedTweet": likedTweet }));
        tweetLikeProducer.produceTweetLike(likedTweet);
      }
    }
  });
});

server.listen(3000, function listening() {
  console.log('Listening on %d', server.address().port);
});
setInterval(() => {
  updateWSClients(JSON.stringify({ "eventType": "time", "time": new Date().toTimeString() }));
}, 1000);

function updateWSClients(message) {
  wss.clients.forEach((client) => {
    client.send(message);
  });

}

// Realtime updates
var sseClients = new sseMW.Topic();



updateSseClients = function (message) {
  sseClients.forEach(function (sseConnection) {
    //   console.log("send sse message global m" + message);
    sseConnection.send(message);
  }
    , this // this second argument to forEach is the thisArg (https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/forEach) 
  );
}



console.log('server running on port 3000');

// heartbeat
setInterval(() => {
  updateSseClients({ "eventType": "tweetEvent", "text": "Heartbeat: " + new Date() + "  #oow17 ", "isARetweet": "N", "author": "Your Node backend system", "hashtag": "HEARTBEAT", "createdAt": null, "language": "en", "tweetId": "1492545590100001b1Un", "tagFilter": "oow17", "originalTweetId": null })
}
  , 2500000
)
var tweetCache = {};
tweetListener.subscribeToTweets((message) => {
  var tweetEvent = JSON.parse(message);
  tweetCache[tweetEvent.tweetId] = tweetEvent;
  updateSseClients(tweetEvent);
}
)

tweetAnalyticsListener.subscribeToTweetAnalytics((message) => {
  console.log("tweet analytic " + message);
  var tweetAnalyticsEvent = JSON.parse(message);
  console.log("tweetAnalyticsEvent " + JSON.stringify(tweetAnalyticsEvent));
  updateSseClients(tweetAnalyticsEvent);
})

tweetLikesAnalyticsListener.subscribeToTweetLikeAnalytics((message) => {
  console.log("tweetLikes analytic " + message);
  var tweetLikesAnalyticsEvent = JSON.parse(message);
  //{"nrs":[{"tweetId":"1495112906610001DCWw","conference":"oow17","count":27,"window":null},{"tweetId":"1492900954165001X6eF","conference":"oow17","count":22,"window":null},{"tweetId":"1496421364049001nhas","conference":"oow17","count":19,"window":null},null]}
  //tweetLikes analytic {"nrs":[{"tweetId":"1495112906610001DCWw","conference":"oow17","count":27,"window":null},{"tweetId":"1492900954165001X6eF","conference":"oow17","count":22,"window":null},{"tweetId":"1496421364049001nhas","conference":"oow17","count":19,"window":null},null]}
  // enrich tweetLikesAnalytic - add tweet text and author
  for (var i = 0; i < 3; i++) {
    if (tweetLikesAnalyticsEvent.nrs[i]) {
      // get tweet from local cache
      var tweetId = tweetLikesAnalyticsEvent.nrs[i].tweetId;
      console.log("tweet id = "+tweetId );
      var tweet = tweetCache[tweetId];
      if (tweet) {
        tweetLikesAnalyticsEvent.nrs[i].text = tweet.text;
        tweetLikesAnalyticsEvent.nrs[i].author = tweet.author;
      }
    }
  }

  tweetLikesAnalyticsEvent.eventType = "tweetLikesAnalytics";
  tweetLikesAnalyticsEvent.conference = tweetLikesAnalyticsEvent.nrs[0].conference  ;
  console.log("tweetLikesAnalyticsEvent " + JSON.stringify(tweetLikesAnalyticsEvent));
  updateSseClients(tweetLikesAnalyticsEvent);
})