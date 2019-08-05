package mainpack;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by costi_000 on 5/23/2016.
 */
public class IterReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double d=0.15;
        double rank= 0.0;

        LinkedList<String> list_values= new LinkedList<String>();
        for( Text value : values){
            list_values.add(value.toString());
        }
        double sum=0.0;
        String friend="";
        for(int i=0;i<list_values.size();i++){
            if(list_values.get(i).contains("!")){
                friend= list_values.get(i).substring(1);
            }else{
                double weight = Double.parseDouble(list_values.get(i));
                sum = sum + (weight);
            }
        }

        rank = d+ (1-d)*sum;

        context.write(new Text(key.toString()+";"+rank),new Text(friend));// key;rank \t !friends

    }
}
