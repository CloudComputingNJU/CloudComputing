import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.Writer;

/**
 * author: Qiao Hongbo
 * time: {$time}
 **/
public class SerializedWriter extends PrintWriter implements Serializable{

    public SerializedWriter(OutputStream out) {
        super(out);
    }
}
