package mainpack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by costi_000 on 5/22/2016.
 */
public class InitReducer extends Reducer<Text, Text, Text, Text>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String k = key.toString();
        System.out.println("key="+k);
        int rank = 1;

        k = k + ";" + rank;
        String out = "";

        String friend;
        for (Text value : values)
        {
            friend=value.toString();
            out = out + friend + ",";
        }

        out = out.substring(0, out.lastIndexOf(','));
        context.write(new Text(k), new Text(out));

    }
}
