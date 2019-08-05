package mainpack;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by costi_000 on 5/21/2016.
 */
public class InitMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override


    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String k= key.toString();
        System.out.println("key="+k);
        String line= value.toString();

        if(line.contains("\t")){
            System.out.println("\tmpike");
            String[] parts= line.split("\t");
            context.write(new Text(parts[0]), new Text(parts[1]));
        }else{
            throw new IllegalArgumentException("Input data format incorrect");
        }
    }
}
