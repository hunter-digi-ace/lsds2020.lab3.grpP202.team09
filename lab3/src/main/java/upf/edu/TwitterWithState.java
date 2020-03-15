package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;

import java.io.IOException;
import java.util.List;

public class TwitterWithState { //este es el 5
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String language = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter With State");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // create a simpler stream of <user, count> for the given language
        final JavaPairDStream<String, Integer> tweetPerUser = stream // IMPLEMENTED
                .filter(s -> s.getLang().equals(language))
                .map(s -> s.getUser().getScreenName())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b);

        // transform to a stream of <userTotal, userName> and get the first 20


        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> reduceFunction =
                (news, current) -> {
                    int sum = current.or(0);
                    for (int i : news){
                        sum +=i;
                    }
                    return Optional.of(sum);
                };


        final JavaPairDStream<Integer, String> tweetsCountPerUser = tweetPerUser // IMPLEMENT ME
                .updateStateByKey(reduceFunction)
                .transformToPair(s -> s.mapToPair(d-> d.swap())
                                       .sortByKey(false));

        tweetsCountPerUser.print(20);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}


/*
usar updateStateByKey operator




 */