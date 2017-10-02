package nl.amis.streams.tweets;

import nl.amis.streams.JsonPOJOSerializer;
import nl.amis.streams.JsonPOJODeserializer;

// generic Java imports
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

// Kafka imports
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
// Kafka Streams related imports
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;

public class App {
    static public class LikedTweetMessage {
        /* the JSON messages produced to the Topic have this structure:
{{"eventType":"tweetEvent","text":"Enjoy insights  at #javaone Servlet 4.0: A New Twist on an Old Favorite","isARetweet":"N","author":"Ed Burns","hashtag":"CON2022","createdAt":null,"language":"en","tweetId":"14926176957820013hZT","tagFilter":"javaone","originalTweetId":null} 
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

    static public class LikedTweetKey {

       public String tweetId ;
       public String conference;
       public LikedTweetKey(String tweetId, String conference) {
         this.tweetId = tweetId;
         this.conference = conference;
       }
        public LikedTweetKey(){};

        public String toString(){
            return "Conference: "+conference+", tweetId:"+tweetId;
        }
    }

    static public class LikedTweetsCount {

       public String tweetId ;
       public String conference;
       public Long count;
       public Object window; // I get a run time error without this property - but I do not use it; perhaps legacy topic definition that is gone after creating a fresh Kafka instance
       public LikedTweetsCount(LikedTweetKey key,Long count) {
         this.tweetId = key.tweetId;
         this.conference = key.conference;
         this.count = count;
       }

        public LikedTweetsCount(){};

        public String toString(){
            return "Conference: "+conference+", tweetId:"+tweetId+" Count "+count.toString();
        }
    }
 

    static public class LikedTweetsTop3 {

       public  LikedTweetsCount[]  nrs = new LikedTweetsCount[4] ;
       public LikedTweetsTop3() {}

        public String toString(){
            String s="Top 3 for "+nrs[0].conference;
            for (int i=0;i<4;i++){
                if (nrs[i]!=null && nrs[i].count !=null ) {
                   s=s+" "+i+". tweetId:"+nrs[i].tweetId+" Count "+nrs[i].count.toString();
                }//if
            }//for
            return s;
        }

    }

    private static final String APP_ID = "tweetlikes-streaming-analysis-app";

static final String  EVENT_HUB_PUBLIC_IP = "192.168.188.102";
static final String  SOURCE_TOPIC_NAME = "tweetLikeTopic";
static final String  SINK_TOPIC_NAME = "tweetLikesAnalyticsTopic";
static final String  ZOOKEEPER_PORT = "2181";
static final String  KAFKA_SERVER_PORT = "9092";

    public static void main(String[] args) {
        System.out.println("Kafka Streams Tweet Likes Analysis");

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(getProperties());
        final Serde < String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();

        // define likedTweetMessageSerde
        Map < String, Object > serdeProps = new HashMap < > ();
        final Serializer < LikedTweetMessage > likedTweetMessageSerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", LikedTweetMessage.class);
        likedTweetMessageSerializer.configure(serdeProps, false);

        final Deserializer < LikedTweetMessage > likedTweetMessageDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", LikedTweetMessage.class);
        likedTweetMessageDeserializer.configure(serdeProps, false);
        final Serde < LikedTweetMessage > likedTweetMessageSerde = Serdes.serdeFrom(likedTweetMessageSerializer, likedTweetMessageDeserializer);

        // define likedTweetsCountSerde
         serdeProps = new HashMap < > ();
        final Serializer < LikedTweetsCount > likedTweetsCountSerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", LikedTweetsCount.class);
        likedTweetsCountSerializer.configure(serdeProps, false);

        final Deserializer < LikedTweetsCount > likedTweetsCountDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", LikedTweetsCount.class);
        likedTweetsCountDeserializer.configure(serdeProps, false);
        final Serde < LikedTweetsCount > likedTweetsCountSerde = Serdes.serdeFrom(likedTweetsCountSerializer, likedTweetsCountDeserializer);


   // define likedTweetsTop3Serde
        serdeProps = new HashMap<String, Object>();
        final Serializer<LikedTweetsTop3> likedTweetsTop3Serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", LikedTweetsTop3.class);
        likedTweetsTop3Serializer.configure(serdeProps, false);

        final Deserializer<LikedTweetsTop3> likedTweetsTop3Deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", LikedTweetsTop3.class);
        likedTweetsTop3Deserializer.configure(serdeProps, false);
        final Serde<LikedTweetsTop3> likedTweetsTop3Serde = Serdes.serdeFrom(likedTweetsTop3Serializer, likedTweetsTop3Deserializer );

        serdeProps = new HashMap<String, Object>();
        final Serializer<LikedTweetKey> likedTweetKeySerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", LikedTweetKey.class);
        likedTweetKeySerializer.configure(serdeProps, false);

        final Deserializer<LikedTweetKey> likedTweetKeyDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", LikedTweetKey.class);
        likedTweetKeyDeserializer.configure(serdeProps, false);
        final Serde<LikedTweetKey> likedTweetKeySerde = Serdes.serdeFrom(likedTweetKeySerializer, likedTweetKeyDeserializer );

        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        // the source of the streaming analysis is the topic with tweets  messages
        KStream<String, LikedTweetMessage> tweetLikesStream = 
                                       kStreamBuilder.stream(stringSerde, likedTweetMessageSerde, SOURCE_TOPIC_NAME);

        // THIS IS THE CORE OF THE STREAMING ANALYTICS:
        // running count of countries per continent, published in topic RunningCountryCountPerContinent
        // KTable<String,Long> runningTweetLikesCount = tweetLikesStream
        //                                          .groupBy((k,tweet) -> tweet.tweetId+"-"+tweet.tagFilter, stringSerde, likedTweetMessageSerde)
        //                                          .count("LikesPerTweet")
        // ;
        
        // runningTweetLikesCount.to(stringSerde, longSerde,  SINK_TOPIC_NAME);
        // runningTweetLikesCount.print(stringSerde, longSerde);

KGroupedStream<LikedTweetKey, LikedTweetMessage> groupedTweetLikeStream2 = tweetLikesStream.groupBy(
    (key, tweetLike) -> new LikedTweetKey(tweetLike.tweetId, tweetLike.tagFilter),
    likedTweetKeySerde, /* key (note: type was modified) */
    likedTweetMessageSerde /* value (note: type was not modified) */
  );
  // count tweet likes grouoed by tweetId
       KTable<Windowed<LikedTweetKey>, Long> countedTweetLikesTable = groupedTweetLikeStream2.count
                (TimeWindows.of(TimeUnit.SECONDS.toMillis(20)),"CountsPerTweetIdAndConference");
                countedTweetLikesTable.toStream().print();

       KTable<LikedTweetsCount,Long> runningTweetLikesCount =countedTweetLikesTable.toStream()
       .map( (windowedLikedTweetKey, count) -> 
             KeyValue.pair(new LikedTweetsCount(windowedLikedTweetKey.key(),  count), count
            )
       )
       .groupByKey(likedTweetsCountSerde, longSerde)
       .count()
     ;
     // print the running count per tweet (indicating conference as well)
     runningTweetLikesCount.toStream()
       .map(  new KeyValueMapper<LikedTweetsCount,Long, KeyValue<String,String>>() {
                  public KeyValue<String,String> apply(LikedTweetsCount key, Long value) {
                     return new KeyValue<>(key+" Hoi!"  , value.toString());
                  }
        })
       .print();


//todo working up to aggregation producing top 3
//KTable<String, LikedTweetsCount> runningTweetLikesCountPerConference = 
countedTweetLikesTable.toStream()
       .map( (windowedLikedTweetKey, count) -> 
             KeyValue.pair(windowedLikedTweetKey.key().conference, new LikedTweetsCount(windowedLikedTweetKey.key(),  count)
            )
       )
       .groupByKey(stringSerde, likedTweetsCountSerde)
       .aggregate(
        new Initializer<Long>() { /* initializer */
      @Override
      public Long apply() {
        return 0L;
      }
    },
    new Aggregator<String, LikedTweetsCount, Long>() { /* adder */
      @Override
      public Long apply(String aggKey, LikedTweetsCount newValue, Long aggValue) {
        return aggValue + newValue.count;
      }
    },
    Serdes.Long(),
    "aggregated-table-store"
    )
                //   .aggregate( 
                //           // first initialize a new LikedTweetsTop3Serde object, initially empty
                //           //LikedTweetsTop3::new
                //           String::new
                //       , // for each tweet in the conference, invoke the aggregator, passing in the continent, the country element and the CountryTop3 object for the continent 
                //          (conference, likedTweetsCount, top3) -> {
                //                    // add the new country as the last element in the nrs array
                //              //      top3.nrs[3]=likedTweetsCount;
                //                        //  sort the array by country size, largest first
                //                 //    Arrays.sort(
                //                 //        top3.nrs, (a, b) -> {
                //                 //          // in the initial cycles, not all nrs element contain a CountryMessage object  
                //                 //          if (a==null)  return 1;
                //                 //          if (b==null)  return -1;
                //                 //          // with two proper CountryMessage objects, do the normal comparison
                //                 //          return Integer.compare(b.size, a.size);
                //                 //        }
                //                 //    );
                //                    // lose nr 4, only top 3 is relevant
                //                //    top3.nrs[3]=null;
                //         //   return (top3);
                //         return ("top3");
                //         }
                //         ,  stringSerde, stringSerde //likedTweetsTop3Serde
                //         ,  "Top3BestLikedTweetsPerConference"
                //        )
            .print();





KTable<String, LikedTweetsTop3> runningTweetLikesCountPerConference = 
countedTweetLikesTable.toStream()
       .map( (windowedLikedTweetKey, count) -> 
             KeyValue.pair(windowedLikedTweetKey.key().conference, new LikedTweetsCount(windowedLikedTweetKey.key(),  count)
            )
       )
       .groupByKey(stringSerde, likedTweetsCountSerde)
       .aggregate(
        new Initializer<LikedTweetsTop3>() { /* initializer */
      @Override
      public LikedTweetsTop3 apply() {
        return new LikedTweetsTop3();
      }
    },
    new Aggregator<String, LikedTweetsCount,LikedTweetsTop3>() { /* adder */
      @Override
      public LikedTweetsTop3 apply(String conference, LikedTweetsCount likedTweetsCount, LikedTweetsTop3 top3) {
         top3.nrs[3] = likedTweetsCount;
                                //  sort the array by likedtweetsCount  count, largest first
                                   Arrays.sort(
                                       top3.nrs, (a, b) -> {
                                         // in the initial cycles, not all nrs element contain a CountryMessage object  
                                         if (a==null)  return 1;
                                         if (b==null)  return -1;
                                         // with two proper LikedTweetsCount objects, do the normal comparison
                                         return Long.compare(b.count, a.count);
                                       }
                                   );
                                   // lose nr 4, only top 3 is relevant
                                  top3.nrs[3]=null;              
         return top3;
      }
    },
likedTweetsTop3Serde,
    "aggregated-table-of-liked-tweets-top3"
    );

        // publish the Top3 messages to Kafka Topic Top3CountrySizePerContinent     
        runningTweetLikesCountPerConference.to(stringSerde, likedTweetsTop3Serde,  "Top3TweetLikesPerConference");

      runningTweetLikesCountPerConference.print();



        System.out.println("Starting Kafka Streams Tweetlikes Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
    kafkaStreams.cleanUp();
    kafkaStreams.start();
        System.out.println("Now started TweetLikesStreams Example");

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));    }




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

//        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") 
        // to work around exception Exception in thread "StreamThread-1" java.lang.IllegalArgumentException: Invalid timestamp -1
        // at org.apache.kafka.clients.producer.ProducerRecord.<init>(ProducerRecord.java:60)
        // see: https://groups.google.com/forum/#!topic/confluent-platform/5oT0GRztPBo
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return settings;
    }

}