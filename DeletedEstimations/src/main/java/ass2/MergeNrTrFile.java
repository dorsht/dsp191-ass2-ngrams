package ass2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MergeNrTrFile {
    public static class MapperClassNrFile extends Mapper<LongWritable, Text,Text, Text> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] splitted = line.toString().split("\t");
            if (splitted.length == 2) {
                context.write (new Text(splitted[0]), new Text(splitted[1]));
            }
        }


        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text, Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (Text value : values){
                sum += Long.parseLong(value.toString());
            }
            context.write(key, new Text("" + sum));

        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    /*public static class CombinerClass extends Reducer<K2,V2,K3,V3> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(K2 key, Iterable<V2> values, Context context) throws IOException,  InterruptedException {
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
*/
    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
        }

    }
}
