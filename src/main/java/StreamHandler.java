import com.mongodb.util.JSON;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;

import scala.Function1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;
import scala.tools.util.SocketServer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * author: Qiao Hongbo
 * time: {$time}
 **/
public class StreamHandler implements Serializable {
    public static final String STREAM_SERVER_HOST = "localhost";
    public static final int STREAM_SERVER_PORT = 9999;
    public boolean hasHandshake = false;

    public void startSocketServer() {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Socket skt = new ServerSocket(9090).accept();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    public void start() throws IOException {
//        Writer writer = null;

        Socket skt = new ServerSocket(9090).accept();
//        skt.setKeepAlive(true);
        System.out.println("accepted: " + skt.getRemoteSocketAddress());
        final Writer writer = new PrintWriter(skt.getOutputStream());
        //获取socket输入流信息
        InputStream in = skt.getInputStream();
        //获取socket输出
        PrintWriter pw = new PrintWriter(skt.getOutputStream(), true);
        //读入缓存(定义一个1M的缓存区)
        byte[] buf = new byte[1024];
        //读到字节（读取输入流数据到缓存）
        int len = in.read(buf, 0, 1024);
        //读到字节数组（定义一个容纳数据大小合适缓存区）
        byte[] res = new byte[len];
        //将buf内中数据拷贝到res中
        System.arraycopy(buf, 0, res, 0, len);
        //打印res缓存内容
        String key = new String(res);

        if (!hasHandshake && key.indexOf("Key") > 0) {
            //握手
            //通过字符串截取获取key值
            key = key.substring(0, key.indexOf("==") + 2);
            key = key.substring(key.indexOf("Key") + 4, key.length()).trim();
            //拼接WEBSOCKET传输协议的安全校验字符串
            key += "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
            //通过SHA-1算法进行更新
            MessageDigest md = null;
            try {
                md = MessageDigest.getInstance("SHA-1");
                md.update(key.getBytes("utf-8"), 0, key.length());

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            byte[] sha1Hash = md.digest();
            //进行Base64加密
            sun.misc.BASE64Encoder encoder = new sun.misc.BASE64Encoder();
            key = encoder.encode(sha1Hash);
            //服务器端返回输出内容
            pw.println("HTTP/1.1 101 Switching Protocols");
            pw.println("Upgrade: websocket");
            pw.println("Connection: Upgrade");
            pw.println("Sec-WebSocket-Accept: " + key);
            pw.println();
            pw.flush();
            //将握手标志更新，只握一次
            hasHandshake = true;
            //服务器返回成功，---握手协议完成，进行TCP通信
//-------------------------------------
//                MongoCursor<Document> itr = sortedCollection.find().iterator();
//            Writer writer = new PrintWriter(skt.getOutputStream());
//                while (itr.hasNext()) {
//                    Document comment = itr.next();
//                writer.write(comment.toJson());
//            int i = 1;
//            while (true) {
//                String str = "count: " + (i++);
//                responseClient(ByteBuffer.wrap(str.getBytes()), true);
//                Thread.sleep(500);
//            }
//


// }
        }
//        final SerializedWriter writer = new SerializedWriter(skt.getOutputStream());


        SparkConf conf = new SparkConf()
//                .setMaster("spark://pyq-master:7077")
                .setMaster("local[2]")
//                .set("SPARK_LOCAL_IP", "114.212.242.132")
//                .set("SPARK_")
                .set("spark.driver.host", "localhost")
                //.set("spark.jars","/home/puyvqi/test/CloudComputing/target/spark-streaming-jingdong-1.0-SNAPSHOT.jar")

                .setAppName("Team13");
        //.setJars(new String[]{"/home/puyvqi/test/CloudComputing/target/spark-streaming-jingdong-1.0-SNAPSHOT.jar","/home/puyvqi/test/CloudComputing/target/dependency/mongo-java-driver-3.4.2.jar"});
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
//        jssc.sparkContext().addJar("/home/puyvqi/test/CloudComputing/target/spark-streaming-jingdong-1.0-SNAPSHOT.jar");
//        jssc.sparkContext().addJar("/home/puyvqi/test/CloudComputing/target/dependency/mongo-java-driver-3.4.2.jar");
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
                return integer + integer2;
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
//        sortedCommentCounts.saveAsHadoopFiles();
        sortedCommentCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> rankLine) throws Exception {
                List<Tuple2<String, Integer>> list = rankLine.collect();
                boolean finalFragment = true;
//                for (;;) {
                for (Tuple2<String, Integer> item : list) {
                    ByteBuffer byteBuf = ByteBuffer.wrap(item.toString().getBytes());
//                    writer.write(item.toString());
//                    writer.write("\n");
//                    writer.flush();
                    OutputStream out = skt.getOutputStream();
                    int first = 0x00;
                    //是否是输出最后的WebSocket响应片段
                    if (finalFragment) {
                        first = first + 0x80;
                        first = first + 0x1;
                    }
                    out.write(first);
                    if (byteBuf.limit() < 126) {
                        out.write(byteBuf.limit());
                    } else if (byteBuf.limit() < 65536) {
                        out.write(126);
                        out.write(byteBuf.limit() >>> 8);
                        out.write(byteBuf.limit() & 0xFF);
                    } else {
                        // Will never be more than 2^31-1
                        out.write(127);
                        out.write(0);
                        out.write(0);
                        out.write(0);
                        out.write(0);
                        out.write(byteBuf.limit() >>> 24);
                        out.write(byteBuf.limit() >>> 16);
                        out.write(byteBuf.limit() >>> 8);
                        out.write(byteBuf.limit() & 0xFF);
                    }
                    // Write the content
                    out.write(byteBuf.array(), 0, byteBuf.limit());
                    out.flush();
                }
            }
        });
//        sortedCommentCounts.comp
//        sortedCommentCounts.saveAsHadoopFiles();
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

        try {
            handler.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
