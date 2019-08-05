package mainpack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by costi_000 on 5/23/2016.
 */
public class DiffReducer2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double max_diff= 0.0;
        boolean flag= false;
        for(Text rank : values){
            if(!flag){
                max_diff=Double.parseDouble(rank.toString());
                flag=true;
            }else{
                double rank_compare= Double.parseDouble(rank.toString());
                if(rank_compare>max_diff){
                    max_diff=rank_compare;
                }
            }
        }
        context.write(new Text(max_diff+""), new Text());
    }
}
