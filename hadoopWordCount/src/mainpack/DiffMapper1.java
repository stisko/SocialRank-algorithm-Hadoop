package mainpack;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by costi_000 on 5/23/2016.
 */
public class DiffMapper1 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts= line.split("\t");

        String[] keyrank = parts[0].split(";");
        String user= keyrank[0];
        double rank= Double.parseDouble(keyrank[1]);

        context.write(new Text(user), new Text(keyrank[1])); // output: key \t rank
    }
}
