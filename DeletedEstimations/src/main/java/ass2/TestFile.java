package ass2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TestFile {

    public static class MapperClass extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        private IntWritable one = new IntWritable(0);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] splitted = value.toString().split("\t");
            context.write(one, new DoubleWritable(Double.parseDouble(splitted[1])));
        }
    }

    public static class ReducerClass extends Reducer<IntWritable,DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
            double sum = 0.0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(new Text("The sum is"), new DoubleWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
//            return 1;
        }
    }

}

