package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.storage.DynamoHashTagRepository;
import upf.edu.util.ConfigUtils;

import java.io.IOException;

public class TwitterHashtags { // este es el 6

    public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // This is needed by spark to write down temporary data
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // <IMPLEMENT ME>
        DynamoHashTagRepository a = new DynamoHashTagRepository();

        final DynamoHashTagRepository table = new DynamoHashTagRepository();
        stream.foreachRDD(s->s.foreach(q->table.write(q)));
        ///aqui falta juntar el stream con algo que llame a
        ///a.write(stream)
        //para que cree en la base de datos por cada tweet


        stream.print();
        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}