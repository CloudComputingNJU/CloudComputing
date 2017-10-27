import com.mongodb.util.JSON;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

/**
 * author: Qiao Hongbo
 * time: {$time}
 **/
public class StreamHandler implements Serializable {
    public static final String STREAM_SERVER_HOST = "114.212.245.176";
    public static final int STREAM_SERVER_PORT = 9999;


    public void start() {
        SparkConf conf = new SparkConf()
                .setMaster("spark://pyq-master:7077")
//                .setMaster("local[2]")
//                .set("SPARK_LOCAL_IP", "114.212.242.132")
//                .set("SPARK_")
                .set("spark.driver.host","114.212.245.176")
                //.set("spark.jars","/home/puyvqi/test/CloudComputing/target/spark-streaming-jingdong-1.0-SNAPSHOT.jar")

                .setAppName("Team13");
                //.setJars(new String[]{"/home/puyvqi/test/CloudComputing/target/spark-streaming-jingdong-1.0-SNAPSHOT.jar","/home/puyvqi/test/CloudComputing/target/dependency/mongo-java-driver-3.4.2.jar"});
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        jssc.sparkContext().addJar("/home/puyvqi/test/CloudComputing/target/spark-streaming-jingdong-1.0-SNAPSHOT.jar");
        jssc.sparkContext().addJar("/home/puyvqi/test/CloudComputing/target/dependency/mongo-java-driver-3.4.2.jar");
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(STREAM_SERVER_HOST, STREAM_SERVER_PORT);
        JavaDStream<Document> docs = lines.map(new Function<String, Document>() {
            @Override
            public Document call(String s) throws Exception {
                return Document.parse(s);
            }
        });
        JavaPairDStream<String, Integer> commentPairs = docs.mapToPair(new PairFunction<Document, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Document document) throws Exception {
                return new Tuple2<>((String) document.get("reference_name"), 1);
            }
        });

        JavaPairDStream<String, Integer> commentCounts = commentPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        JavaPairDStream<Integer, String> reverseCommentCounts = commentCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> commentCount) throws Exception {
                return commentCount.swap();
            }
        });

        JavaPairDStream<Integer, String> sortedReverseCommentCounts = reverseCommentCounts.transformToPair(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
            @Override
            public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> reverse) throws Exception {
                return reverse.sortByKey(false);
            }
        });

        JavaPairDStream<String, Integer> sortedCommentCounts = sortedReverseCommentCounts.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> commentCount) throws Exception {
                return commentCount.swap();
            }
        });

        sortedCommentCounts.print(3);
//        sortedCommentCounts
//        sortedCommentCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
//            @Override
//            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
//                Map<String, Integer> rs = rdd.collectAsMap();
//            }
//        });
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
       PropertyConfigurator.configure("conf/log4j.properties");
        Logger logger = Logger.getLogger(StreamHandlerLambda.class);
        logger.debug("debug");


        StreamHandler handler = new StreamHandler();

        handler.start();
    }
}
