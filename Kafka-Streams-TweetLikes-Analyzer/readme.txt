created using maven with:

mvn archetype:generate -DgroupId=nl.amis.streams.tweets -DartifactId=Kafka-Streams-Tweets-Analyzer -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

updated pom.xml

<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
   <version>0.11.0.1</version>
 </dependency>

and

<build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.1</version>
        <configuration>
          <source>1.8</source>
          <target>1.8</target>
        </configuration>
      </plugin>
    </plugins>
  </build>

mvn install dependency:copy-dependencies

to run the Kafka Stream App:
java -cp target/Kafka-Streams-TweetLikes-Analyzer-1.0-SNAPSHOT.jar;target/dependency/* nl.amis.streams.tweets.App


(see blog https://technology.amis.nl/2017/02/11/getting-started-with-kafka-streams-building-a-streaming-analytics-java-application-against-a-kafka-topic/)