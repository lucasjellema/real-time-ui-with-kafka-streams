package nl.amis.streams.tweets;

import nl.amis.streams.JsonPOJOSerializer;
import nl.amis.streams.JsonPOJODeserializer;

// generic Java imports
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
// Kafka imports
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
// Kafka Streams related imports
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

public class App {
    static public class TweetMessage {
        /* the JSON messages produced to the Topic have this structure:
{"eventType":"tweetEvent","text":"RT @AccentureTech: Proud to be an #OOW17 Grande Sponsor and @Oracle"s #1 systems integrator globally 12 years in a row.… "
,"isARetweet":"y","author":"Katie Petroskey","hashtags":[{"text":"OOW17","indices":[34,40]}],"createdAt":"Fri Sep 29 13:27:36 +0000 2017","language":"en","tweetId":913757152774381600,"originalTweetId":913463349744193500}  
        this class needs to have at least the corresponding fields to deserialize the JSON messages into
        */

        public String eventType;
        public String text;
        public String tweetId;
        public String isARetweet;
        public String author;
        public String tagFilter;
        public String hashtag;
        public String language;
        public String createdAt;
        public String originalTweetId; 

        public String toString() {
            return eventType+ " :"+text+" #"+tagFilter+" #"+hashtag;
        }
    }

    private static final String APP_ID = "tweets-streaming-analysis-app";

static final String  EVENT_HUB_PUBLIC_IP = "192.168.188.102";
static final String  SOURCE_TOPIC_NAME = "tweetsTopic";
static final String  SINK_TOPIC_NAME = "tweetAnalyticsTopic";
static final String  ZOOKEEPER_PORT = "2181";
static final String  KAFKA_SERVER_PORT = "9092";

    public static void main(String[] args) {
        System.out.println("Kafka Streams Tweet Analysis");

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(getProperties());
        final Serde < String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();

        // define TweetMessageSerde
        Map < String, Object > serdeProps = new HashMap < > ();
        final Serializer < TweetMessage > tweetMessageSerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", TweetMessage.class);
        tweetMessageSerializer.configure(serdeProps, false);

        final Deserializer < TweetMessage > tweetMessageDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", TweetMessage.class);
        tweetMessageDeserializer.configure(serdeProps, false);
        final Serde < TweetMessage > tweetMessageSerde = Serdes.serdeFrom(tweetMessageSerializer, tweetMessageDeserializer);

        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        // the source of the streaming analysis is the topic with tweets  messages
        KStream<String, TweetMessage> tweetStream = 
                                       kStreamBuilder.stream(stringSerde, tweetMessageSerde, SOURCE_TOPIC_NAME);

        // THIS IS THE CORE OF THE STREAMING ANALYTICS:
        // running count of countries per continent, published in topic RunningCountryCountPerContinent
        KTable<String,Long> runningTweetsCount = tweetStream
                                                 .groupBy((k,tweet) -> tweet.tagFilter, stringSerde, tweetMessageSerde)
                                                 .count("Conference")
        ;
        
        runningTweetsCount.to(stringSerde, longSerde,  SINK_TOPIC_NAME);
        runningTweetsCount.print(stringSerde, longSerde);



        System.out.println("Starting Kafka Streams Tweets Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.start();
        System.out.println("Now started TweetsStreams Example");
    }




    private static Properties getProperties() {
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        // Kafka bootstrap server (broker to talk to); the host name for my VM running Kafka, port is where the (single) broker listens 
        // Apache ZooKeeper instance keeping watch over the Kafka cluster;  
            settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, EVENT_HUB_PUBLIC_IP+":"+KAFKA_SERVER_PORT);
            // Apache ZooKeeper instance keeping watch over the Kafka cluster;
            settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, EVENT_HUB_PUBLIC_IP+":"+ZOOKEEPER_PORT);

            settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            settings.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\temp");

        settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        // to work around exception Exception in thread "StreamThread-1" java.lang.IllegalArgumentException: Invalid timestamp -1
        // at org.apache.kafka.clients.producer.ProducerRecord.<init>(ProducerRecord.java:60)
        // see: https://groups.google.com/forum/#!topic/confluent-platform/5oT0GRztPBo
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return settings;
    }

}