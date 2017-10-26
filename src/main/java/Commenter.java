import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * author: Qiao Hongbo
 * time: {$time}
 **/
public class Commenter implements Runnable{
    public static final int COMMENTER_PORT = 9999;
    private MongoCollection<Document> sortedCollection;

    public Commenter(){
        try{
            // 连接到 mongodb 服务
            MongoClient mongoClient = new MongoClient( "zc-slave" , 27017 );

            // 连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("jd");
            sortedCollection = mongoDatabase.getCollection("comment_list_sorted");
            System.out.println("Connect to database successfully");

        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }

    public void run() {
        System.out.println("waiting for connection on port: "+ COMMENTER_PORT);
        try {
            Socket skt = new ServerSocket(COMMENTER_PORT).accept();
            System.out.println("connected by "+skt.getRemoteSocketAddress());
            MongoCursor<Document> itr= sortedCollection.find().iterator();
            Writer writer = new PrintWriter(skt.getOutputStream());
            while(itr.hasNext()){
                Document comment = itr.next();
                writer.write(comment.toJson());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Thread(new Commenter()).start();
    }
}
