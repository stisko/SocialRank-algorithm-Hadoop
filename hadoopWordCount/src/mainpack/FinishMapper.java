package mainpack;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by costi_000 on 5/24/2016.
 */
public class FinishMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line =  value.toString();
        String[] parts= line.split("\t");

        String[] keyrank= parts[0].split(";");
        double rank=- Double.parseDouble(keyrank[1]);
        context.write(new DoubleWritable(rank), new Text(keyrank[0]));

    }
}
