package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json4s.jackson.Json;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import upf.edu.util.LanguageMapUtils;

import java.io.IOException;

public class TwitterStateless {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String input = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);


        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Stateless Exercise");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // Read the language map file by line
        final JavaRDD<String> languageMapLines = jsc
                .sparkContext()
                .textFile(input);
        // transform it to the expected RDD like in Lab 4
        final JavaPairRDD<String, String> languageMap = LanguageMapUtils
                .buildLanguageMap(languageMapLines);
        // prepare the output
        final JavaPairDStream<String, Integer> languageRankStream = stream // IMPLEMENTED
                .map(s -> s.getLang())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .transformToPair(s -> s.leftOuterJoin(languageMap)
                                 .filter(f -> f._2._2.isPresent())
                                 .mapToPair(d -> new Tuple2<String,Integer>(d._2._2.get(),d._2._1)))
                .reduceByKey((a, b) -> a + b);

        // print first 10 results
        languageRankStream.print(10);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
