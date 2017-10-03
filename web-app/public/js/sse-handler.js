// assume that API service is published on same server that is the server of this HTML file
var source = new EventSource("../updates");
source.onmessage = function (event) {
    var data = JSON.parse(event.data);
    if (data.eventType == 'tweetAnalytics') {
        var span = document.getElementById(data.conference + "TweetCount");
        span.innerHTML = data.tweetCount;
    } else {
        if (data.eventType == 'tweetLikesAnalytics') {
            var conference = data.conference;
            var timeCell = document.getElementById(conference + "Top3LikesTime");
            timeCell.innerHTML = new Date().toLocaleTimeString();
            var top3LikesTable = document.getElementById(conference + "Top3LikesTimeTable");
            while (top3LikesTable.rows.length > 1) {
                top3LikesTable.deleteRow(1);
            }
            for (i = 0; i < data.nrs.length; i++) {
                if (data.nrs[i] && data.nrs[i].count) {
                    var row = top3LikesTable.insertRow(i + 1); // after header
                    var tweetCell = row.insertCell(0);
                    var authorCell = row.insertCell(1);
                    var countCell = row.insertCell(2);
                    authorCell.innerHTML = data.nrs[i].author;
                    tweetCell.innerHTML = data.nrs[i].text;
                    countCell.innerHTML = data.nrs[i].count;
                }
            }//for

        } else {
            var table = document.getElementById("tweetsTable");
            var row = table.insertRow(1); // after header
            var likeCell = row.insertCell(0);
            var conferenceCell = row.insertCell(1);
            var authorCell = row.insertCell(2);
            var tweetCell = row.insertCell(3);
            var tagCell = row.insertCell(4);
            conferenceCell.innerHTML = data.tagFilter;
            authorCell.innerHTML = data.author;
            tweetCell.innerHTML = data.text;
            tagCell.innerHTML = data.hashtag;
            likeCell.innerHTML = `<img src="images/like-tweet.jpg" width="40" onclick="like('${data.tweetId}')"/>`;
        }
    }
};//onMessage
