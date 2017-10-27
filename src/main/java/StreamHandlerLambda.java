import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;
import scala.Tuple2;

/**
 * author: Qiao Hongbo
 * time: {$time}
 **/
public class StreamHandlerLambda {
    public static final String STREAM_SERVER_HOST = "localhost";
    public static final int STREAM_SERVER_PORT = 9999;


    public void start() {
        SparkConf conf = new SparkConf()
                .setMaster("spark://pyq-master:7077")
//                .set("SPARK_LOCAL_IP", "114.212.242.132")
//                .set("SPARK_")
                .setAppName("Team13");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(STREAM_SERVER_HOST, STREAM_SERVER_PORT);
        JavaDStream<Document> docs = lines.map(Document::parse);
        JavaPairDStream<String, Integer> commentPairs = docs.mapToPair(doc -> new Tuple2<>("" + doc.get("comment_id"), 1));
        JavaPairDStream<String, Integer> commentCounts = commentPairs.reduceByKey((cnt1, cnt2) -> cnt1 + cnt2);
        commentCounts.print(10);
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
        logger.error("error");

        StreamHandlerLambda handler = new StreamHandlerLambda();

        handler.start();
    }
}
