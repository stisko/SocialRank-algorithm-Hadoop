package mainpack;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by costi_000 on 5/23/2016.
 */
public class IterMapper extends Mapper<LongWritable, Text, Text, Text>{


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line= value.toString();
        String[] parts= line.split("\t");
        //TODO an kapoios den exei pollous filous na psaxnei an uparxei to ,
        String[] friends;

        if(parts.length!=2){//otan kapoios den akolouthei kanenan
            return;
        }

        if(parts[1].contains(",")){
            friends= parts[1].split(",");

        }else{
            friends= new String[1];
            friends[0]= parts[1];
        }


        String[] keyrank = parts[0].split(";");
        String user= keyrank[0];
        double rank= Double.parseDouble(keyrank[1]);

        double weight= rank/ friends.length;

        for(int i=0;i<friends.length;i++){
            context.write(new Text(friends[i]),new Text(weight+""));
        }
        context.write(new Text(user), new Text("!"+parts[1]));
    }
}
