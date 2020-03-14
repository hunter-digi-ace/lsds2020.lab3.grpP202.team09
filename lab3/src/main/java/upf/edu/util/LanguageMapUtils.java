package upf.edu.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

public class LanguageMapUtils {

    public static JavaPairRDD<String, String> buildLanguageMap(JavaRDD<String> lines) {
        // IMPLEMENT ME
        JavaPairRDD<String, String> languajes = lines
                .map(s -> Arrays.asList(s.split("\\t")))
                .filter(s -> s.get(1).length() == 2)
                .mapToPair(s -> new Tuple2<String, String>(s.get(1),s.get(2)))
                .coalesce(1);
        return languajes;
    }
}
