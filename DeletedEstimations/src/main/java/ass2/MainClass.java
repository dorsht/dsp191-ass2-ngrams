package ass2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.LocalTime;

import java.io.IOException;

import static ass2.NCounter.Counter.N_COUNTER;

public class MainClass {

    private static String bucketOutputPath;

    private static String setPaths (Job job, String inputPath) throws IOException {
        FileInputFormat.addInputPath(job, new Path(inputPath));
        String path = bucketOutputPath + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }

    private static String splitJobMaker (Job job, String filePath) throws IOException {
        job.setJarByClass(CreateSplitFile.class);
        job.setMapperClass(CreateSplitFile.MapperClass.class);
        job.setPartitionerClass(CreateSplitFile.PartitionerClass.class);
        job.setCombinerClass(CreateSplitFile.CombinerClass.class);
        job.setReducerClass(CreateSplitFile.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongMapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongMapWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        return setPaths(job, filePath);
    }
    private static String NrJobMaker (Job job, String filePath) throws IOException {
        job.setJarByClass(CreateNrFile.class);
        job.setMapperClass(CreateNrFile.MapperClass.class);
        job.setPartitionerClass(CreateNrFile.PartitionerClass.class);
        job.setCombinerClass(CreateNrFile.ReducerClass.class);
        job.setReducerClass(CreateNrFile.ReducerClass.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        return setPaths(job, filePath);
    }
    private static String TrJobMaker (Job job, String filePath) throws IOException {
        job.setJarByClass(CreateTrFile.class);
        job.setMapperClass(CreateTrFile.MapperClass.class);
        job.setPartitionerClass(CreateTrFile.PartitionerClass.class);
        job.setCombinerClass(CreateTrFile.ReducerClass.class);
        job.setReducerClass(CreateTrFile.ReducerClass.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        return setPaths(job, filePath);
    }
    private static String MergeNrTrIntermediateJobMaker(Job job, String splitFilePath, String Nr0FilePath){
        job.setJarByClass(MergeNrTrIntermediateFile.class);
        job.setReducerClass(MergeNrTrIntermediateFile.ReducerClass.class);
        job.setPartitionerClass(MergeNrTrIntermediateFile.PartitionerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(splitFilePath),
                TextInputFormat.class, MergeNrTrIntermediateFile.MapperClassSplittedFile.class);
        MultipleInputs.addInputPath(job, new Path (Nr0FilePath),
                TextInputFormat.class, MergeNrTrIntermediateFile.MapperClassNrFile.class);
        String path = bucketOutputPath + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path(path));
        return path;
    }

    private static String MergeNrTrJobMaker(Job job, String splitFilePath, String Nr1FilePath){
        job.setJarByClass(MergeNrTrFile.class);
        job.setReducerClass(MergeNrTrFile.ReducerClass.class);
        job.setPartitionerClass(MergeNrTrFile.PartitionerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(splitFilePath),
                TextInputFormat.class, MergeNrTrFile.MapperClassNrFile.class);
        MultipleInputs.addInputPath(job, new Path (Nr1FilePath),
                TextInputFormat.class, MergeNrTrFile.MapperClassNrFile.class);
        String path = bucketOutputPath + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path(path));
        return path;
    }

    private static String MergeJobMaker(Job job, String mergeNrPath, String mergeTrPath){
        job.setJarByClass(MergeFile.class);
        job.setReducerClass(MergeFile.ReducerClass.class);
        job.setPartitionerClass(MergeFile.PartitionerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(mergeNrPath),
                TextInputFormat.class, MergeFile.MapperClassNr.class);
        MultipleInputs.addInputPath(job, new Path (mergeTrPath),
                TextInputFormat.class, MergeFile.MapperClassTr.class);
        String path = bucketOutputPath + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path(path));
        return path;
    }

    private static String SortJobMaker(Job job, String mergePath, String outputPath) throws IOException {
        job.setJarByClass(SortFile.class);
        job.setMapperClass(SortFile.MapperClass.class);
        job.setPartitionerClass(SortFile.PartitionerClass.class);
        job.setSortComparatorClass(NgramSortComparator.class);
        job.setReducerClass(SortFile.ReducerClass.class);
        job.setMapOutputKeyClass(NgramProbComparable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NgramProbComparable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(mergePath));
        String path = outputPath + "/" + job.getJobName() + "-" +
                LocalTime.now().toString().replace(":","-");
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // First stage - split the corpus into 2 parts
        if (args.length<2){
            System.out.println("please provide input path and output path");
            System.exit(1);
        }

        String input = args[0];
        String output = args[1];
        bucketOutputPath = output;
        bucketOutputPath = bucketOutputPath.concat("/" + LocalTime.now().toString().replace(":","-")+"/");

        Configuration splitFileConf = new Configuration();
        final Job splitFile = Job.getInstance(splitFileConf, "Split");
        String splitFilePath = splitJobMaker(splitFile, input);
        if (splitFile.waitForCompletion(true)){
            System.out.println("First   stage - done! :)");
        }
        else{
            System.out.println("First   stage failed :(");
            System.exit(1);
        }
        Counters counters = splitFile.getCounters();
        Counter counter = counters.findCounter(N_COUNTER);
        long N = counter.getValue();

        // Second stage - count Nr0 and Nr1

        Configuration Nr0Conf = new Configuration();
        Nr0Conf.setBoolean("col", true);
        final Job Nr0File = Job.getInstance(Nr0Conf, "Nr0");
        String Nr0FilePath = NrJobMaker(Nr0File, splitFilePath);

        Configuration Nr1Conf = new Configuration();
        Nr1Conf.setBoolean("col", false);
        final Job Nr1File = Job.getInstance(Nr1Conf, "Nr1");
        String Nr1FilePath = NrJobMaker(Nr1File, splitFilePath);

        boolean Nr0Done = Nr0File.waitForCompletion(true);
        boolean Nr1Done = Nr1File.waitForCompletion(true);

        if (Nr0Done & Nr1Done){
            System.out.println("Second  stage - done!! :)");
        }
        else if (!Nr0Done){
            System.out.println("Second  stage - Nr0 failed");
            System.exit(1);
        }
        else{
            System.out.println("Second  stage - Nr1 failed");
            System.exit(1);
        }

        // Third stage - count Tr10 and Tr01

        Configuration Tr01Conf = new Configuration();
        Tr01Conf.setBoolean("direction", true);
        final Job Tr01File = Job.getInstance(Tr01Conf, "Tr01");
        String Tr01FilePath = TrJobMaker(Tr01File, splitFilePath);

        Configuration Tr10Conf = new Configuration();
        Tr10Conf.setBoolean("direction", false);
        final Job Tr10File = Job.getInstance(Tr10Conf, "Tr10");
        String Tr10FilePath = TrJobMaker(Tr10File, splitFilePath);

        boolean Tr01Done = Tr01File.waitForCompletion(true);
        boolean Tr10Done = Tr10File.waitForCompletion(true);

        if (Tr01Done & Tr10Done){
            System.out.println("Third   stage - done!!! :)");
        }
        else if (!Tr01Done){
            System.out.println("Third   stage - Tr01 failed :(");
            System.exit(1);
        }
        else{
            System.out.println("Third   stage - Tr10 failed :(");
            System.exit(1);
        }

        // Fourth Stage merge Nr files into one

        Configuration mergeNrIntermediateNr0Conf = new Configuration();
        mergeNrIntermediateNr0Conf.setInt("index",1);
        final Job mergeNr0IntermediateCreateFile = Job.getInstance(mergeNrIntermediateNr0Conf, "MergeNr0Intermediate");
        String mergeNr0IntermediatePath = MergeNrTrIntermediateJobMaker(mergeNr0IntermediateCreateFile,
                                                                        splitFilePath, Nr0FilePath);
        if (mergeNr0IntermediateCreateFile.waitForCompletion(true)){
            System.out.println("Fourth  stage - Intermediate merge NR0 done!!!! :)");
        }
        else{
            System.out.println("Fourth  stage - Intermediate merge NR0 failed :(");
            System.exit(1);
        }


        Configuration mergeNrIntermediateNr1Conf = new Configuration();
        mergeNrIntermediateNr1Conf.setInt("index",2);
        final Job mergeNr1IntermediateCreateFile = Job.getInstance(mergeNrIntermediateNr0Conf, "MergeNr1Intermediate");
        String mergeNr1IntermediatePath = MergeNrTrIntermediateJobMaker(mergeNr1IntermediateCreateFile,
                splitFilePath, Nr1FilePath);
        if (mergeNr1IntermediateCreateFile.waitForCompletion(true)){
            System.out.println("Fourth  stage - Intermediate merge NR1 done!!!! :)");
        }
        else{
            System.out.println("Fourth  stage - Intermediate merge NR1 failed :(");
            System.exit(1);
        }


        Configuration mergeNrConf = new Configuration();
        final Job mergeNrCreateFile = Job.getInstance(mergeNrConf, "MergeNr");
        String mergeNrPath = MergeNrTrJobMaker(mergeNrCreateFile,mergeNr0IntermediatePath , mergeNr1IntermediatePath);
        if (mergeNrCreateFile.waitForCompletion(true)){
            System.out.println("Fourth  stage - Merge NR's done!!!! :)");
        }
        else{
            System.out.println("Fourth  stage - Merge NR's failed :(");
            System.exit(1);
        }

        // Fifth Stage merge Tr files into one

        Configuration mergeTrIntermediateTr0Conf = new Configuration();
        mergeTrIntermediateTr0Conf.setInt("index",1);
        final Job mergeTr0IntermediateCreateFile = Job.getInstance(mergeTrIntermediateTr0Conf, "MergeTr0Intermediate");
        String mergeTr0IntermediatePath = MergeNrTrIntermediateJobMaker(mergeTr0IntermediateCreateFile,
                splitFilePath, Tr01FilePath);
        if (mergeTr0IntermediateCreateFile.waitForCompletion(true)){
            System.out.println("Fifth   stage - Intermediate merge TR01 done!!!!! :)");
        }
        else{
            System.out.println("Fifth   stage - Intermediate merge TR01 failed :(");
            System.exit(1);
        }


        Configuration mergeTrIntermediateTr1Conf = new Configuration();
        mergeTrIntermediateTr1Conf.setInt("index",2);
        final Job mergeTr1IntermediateCreateFile = Job.getInstance(mergeTrIntermediateTr1Conf, "MergeTr1Intermediate");
        String mergeTr1IntermediatePath = MergeNrTrIntermediateJobMaker(mergeTr1IntermediateCreateFile,
                splitFilePath, Tr10FilePath);
        if (mergeTr1IntermediateCreateFile.waitForCompletion(true)){
            System.out.println("Fifth   stage - Intermediate merge TR10 done!!!!! :)");
        }
        else{
            System.out.println("Fifth   stage - Intermediate merge TR10 failed :(");
            System.exit(1);
        }


        Configuration mergeTrConf = new Configuration();
        final Job mergeTrCreateFile = Job.getInstance(mergeTrConf, "MergeTr");
        String mergeTrPath = MergeNrTrJobMaker(mergeTrCreateFile,mergeTr0IntermediatePath , mergeTr1IntermediatePath);
        if (mergeTrCreateFile.waitForCompletion(true)){
            System.out.println("Fifth   stage - Merge TR's done!!!!!! :)");
        }
        else{
            System.out.println("Fifth   stage - Merge TR's failed :(");
            System.exit(1);
        }

        // Sixth Stage - create output file with ngrams and probabilities

        Configuration mergeConf = new Configuration();
        mergeConf.setLong("N", N);
        final Job mergeFile = Job.getInstance(mergeConf, "merge");
        String mergePath = MergeJobMaker (mergeFile, mergeNrPath, mergeTrPath);
        if (mergeFile.waitForCompletion(true)){
            System.out.println("Sixth   stage - done!!!!!! :)");
        }
        else{
            System.out.println("Sixth   stage - failed :(");
            System.exit(1);
        }

        // Seventh Stage - sort

        Configuration sortConf = new Configuration();
        final Job sortJob = Job.getInstance(sortConf, "Result");
        String sortPath = SortJobMaker(sortJob, mergePath, output);
        if (sortJob.waitForCompletion(true)) {
            System.out.println("Seventh stage - done!!!!!!! :)");
        } else {
            System.out.println("Seventh stage - failed :(");
            System.exit(1);
        }
    }
}
