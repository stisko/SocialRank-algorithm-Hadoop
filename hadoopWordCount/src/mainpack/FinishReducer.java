package mainpack;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by costi_000 on 5/24/2016.
 */

public class FinishReducer extends Reducer<DoubleWritable,Text,Text,Text> {

    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double rank=- Double.parseDouble(key.toString());
        for(Text value : values){
            context.write(value,new Text(rank+"") );
        }
    }

}
