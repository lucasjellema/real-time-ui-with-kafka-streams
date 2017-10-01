var wsUri = "ws://" + document.location.host;
var websocket = new WebSocket(wsUri);

websocket.onmessage = function (evt) { onMessage(evt) };
websocket.onerror = function (evt) { onError(evt) };
websocket.onopen = function (evt) { onOpen(evt) };

function onMessage(evt) {
    console.log("received over websockets: " + evt.data);
    var message = JSON.parse(evt.data);
    if (message.eventType == 'time') {
        writeToScreen(message.time);
    }
    if (message.eventType == 'tweetLiked') {
        var likedTweet = message.likedTweet;
        handleFreshTweetLike(likedTweet);
        writeToScreen("TweetLiked: " + likedTweet.text);
    }

}

function handleFreshTweetLike(likedTweet){
    var table = document.getElementById("recentLikesTable");
    var row = table.insertRow(1); // after header
    var timeCell = row.insertCell(0);
    var conferenceCell = row.insertCell(1);
    var tweetCell = row.insertCell(2);
    var tagCell = row.insertCell(3);
    timeCell.innerHTML = new Date().toLocaleTimeString();
    conferenceCell.innerHTML = likedTweet.tagFilter;
    tweetCell.innerHTML = likedTweet.text;
    tagCell.innerHTML = likedTweet.hashtag;

    var rows = table.childNodes[1].childNodes;  // childNodes[1] us tableBody
    if (rows.length>6) {
        table.childNodes[1].removeChild(rows[6]);
    }
}

function onError(evt) {
    writeToScreen('<span style="color: red;">ERROR:</span> ' + evt.data);
}

function onOpen() {
    writeToScreen("Connected to " + wsUri);
}

// For testing purposes
var output = document.getElementById("output");

function writeToScreen(message) {
    if (output == null) { output = document.getElementById("output"); }
    output.innerHTML = message + "";
}

function like(tweetId) {
    sendText(JSON.stringify({ "eventType": "tweetLike", "tweetId": tweetId }));
}

function sendText(json) {
    console.log("sending text: " + json);
    websocket.send(json);
}