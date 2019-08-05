package mainpack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by costi_000 on 5/23/2016.
 */
public class DiffReducer1 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double[] ranks_array= new double[2];
        int i=0;
        for(Text ranks: values){
            if(i<=1){
                ranks_array[i]= Double.parseDouble(ranks.toString());
            }
            i++;
        }
        double diff= Math.abs(ranks_array[0]-ranks_array[1]);

        context.write(new Text(diff+""),new Text()); // output difference
    }
}
