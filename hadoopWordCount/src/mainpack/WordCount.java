package mainpack;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {



    public static void main(String[] args) throws Exception {

        long start,end,time_elapsed;
        if(args[0].equals("init")) {
            init(args[1],args[2],Integer.parseInt(args[3]));
        }else if(args[0].equals("iter")) {
            iter(args[1], args[2], Integer.parseInt(args[3]));
        }else if(args[0].equals("diff")){
            diff(args[1],args[2],args[3],Integer.parseInt(args[4]));
        }else if(args[0].equals("finish")){
            finish(args[1],args[2],Integer.parseInt(args[3]));
        }else if(args[0].equals("composite")){
            start = System.currentTimeMillis();
            composite(args[1],args[2],args[3],args[4],args[5],Double.parseDouble(args[6]),Integer.parseInt(args[7]));
            end = System.currentTimeMillis();
            time_elapsed=end-start;
            System.out.println("Time elapsed in ms: "+time_elapsed);
        }else if(args[0].equals("read")){
            double diff= findDiffernce(args[1]);
            System.out.println(diff);
        }else{
            System.out.println("Exeis kanei lathos!");
        }
    }

    public static void init(String input, String output, int numReducers) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "init");
        job.setJarByClass(WordCount.class);

        job.setNumReduceTasks(numReducers); // Sets the number of reducers

        FileInputFormat.addInputPath(job, new Path(input)); // Adds input and output paths
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(InitMapper.class); // Initializes the Mapper and Reducer Classes
        job.setReducerClass(InitReducer.class);

        job.setMapOutputKeyClass(Text.class); // Sets the input output types for the Mapper and Reducer
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Prints message on successful completion or in case of an error
        System.out.print(job.waitForCompletion(true) ? "Init Job Completed"
                : "Init Job Error");
    }

    public static void iter(String input, String output, int numReducers) throws IOException, ClassNotFoundException, InterruptedException{
        System.out.println("Iter Job Started");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "iter");
        job.setJarByClass(WordCount.class); // Sets the Driver class
        job.setNumReduceTasks(numReducers); // Sets the number of reducers

        FileInputFormat.addInputPath(job, new Path(input)); // Adds input and
        // output paths
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(IterMapper.class); // Sets the Mapper and Reducer
        // classes
        job.setReducerClass(IterReducer.class);

        job.setMapOutputKeyClass(Text.class); // Sets the Mapper and Reducer
        // output types
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Prints message on successful completion or in case of an error
        System.out.print(job.waitForCompletion(true) ? "Iter Job Completed"
                : "Iter Job Error");
    }

    public static void diff(String input1, String input2, String output, int numReducers) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "diff");
        job.setJarByClass(WordCount.class);
        job.setNumReduceTasks(numReducers);

        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        deleteDir("DiffReducer1_Output");
        FileOutputFormat.setOutputPath(job, new Path("DiffReducer1_Output"));

        job.setMapperClass(DiffMapper1.class);
        job.setReducerClass(DiffReducer1.class);

        job.setMapOutputKeyClass(Text.class); // Sets output classes for Mapper and Reducers for Job 1
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if(job.waitForCompletion(true)){
            System.out.println("Diff2 Started");
            Job job2= Job.getInstance(conf,"diff2");
            job2.setJarByClass(WordCount.class);

            FileInputFormat.addInputPath(job2, new Path("DiffReducer1_Output")); // Adds the temporary directory "tempdiff" as input
            FileOutputFormat.setOutputPath(job2, new Path(output)); // Sets output

            job2.setMapperClass(DiffMapper2.class); // Sets Mapper and Reducer classes for second job
            job2.setReducerClass(DiffReducer2.class);

            job2.setMapOutputKeyClass(Text.class); // Sets output class for second job
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);


            System.out
                    .print(job2.waitForCompletion(true) ? "Diff Job Completed"
                            : "Diff Job Error");


        }

    }

    public static void finish(String input, String output, int numReducers) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "finish");
        job.setJarByClass(WordCount.class);
        job.setNumReduceTasks(numReducers);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output)); // Sets output

        job.setMapperClass(FinishMapper.class);
        job.setReducerClass(FinishReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out
                .print(job.waitForCompletion(true) ? "Finish Job Completed"
                        : "Finish Job Error");


    }

    public static void composite(String input, String output, String intermDir1, String intermDir2, String diffDir,double threshold, int numReducers) throws Exception {
        boolean flag=false;
        int cnt=0,i=0;
        init(input,intermDir1,numReducers);
        cnt++;
        double difference=30.0;

        while(difference>=threshold){
            if(!flag){
                iter(intermDir1,intermDir2,numReducers);

            }else{
                iter(intermDir2,intermDir1,numReducers);
            }

            if(i%3==0){
                deleteDir(diffDir);
                diff(intermDir1,intermDir2,diffDir, numReducers);
                // difference=readDifference(diffDir);
                difference=findDiffernce(diffDir);
                // delete directory


            }

            if(!flag){
                //TODO delete interm1
                deleteDir(intermDir1);
                flag=true;
            }else{
                deleteDir(intermDir2);
                //TODO delete interm2
                flag=false;
            }
            cnt++;
            i++;
        }

        if(flag){
            //deleteDir(intermDir1);
            // delete interm1
            cnt++;
            finish(intermDir2,output,numReducers);
        }else{
           //deleteDir(intermDir2);
            // delete interm2
            finish(intermDir1,output,numReducers);
            cnt++;
        }

        System.out.println("TEleiwse i composite");
        System.out.println(cnt);


    }




    static void deleteDir(String path) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        Path path_delete = new Path(path);
        if (fs.exists(path_delete))
            fs.delete(path_delete, true);

        fs.close();
    }

    static double findDiffernce(String input) throws IOException {
        double difference=0.0;
        Configuration conf = new Configuration();
        Path path= new Path(input);

        FileSystem fs = FileSystem.get(URI.create(input), conf);
        if(fs.exists(path)){
            System.out.println("mpike exists");
            FileStatus[] fileStatus = fs.listStatus(path);
            for(int i=0;i<fileStatus.length;i++){
                System.out.println("mpike for----"+ fileStatus[i].getPath().getName());


                if(fileStatus[i].getPath().getName().startsWith("part-r")){
                    System.out.println("mpike starts with");
                    FSDataInputStream inpStream= fs.open(fileStatus[i].getPath());
                    BufferedReader d = new BufferedReader(new InputStreamReader(inpStream));
                    String line = d.readLine();
                    if(line!=null){
                        difference= Double.parseDouble(line);
                    }
                    d.close();
                }
            }
        }
        fs.close();
        return difference;
    }

}