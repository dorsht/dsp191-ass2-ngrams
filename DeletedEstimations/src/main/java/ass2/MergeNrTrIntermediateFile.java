package ass2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

public class MergeNrTrIntermediateFile {
    public static class MapperClassSplittedFile extends Mapper<LongWritable, Text, Text, Text> {
        private int index;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            index = context.getConfiguration().getInt("index", -1);
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] splitted = line.toString().split("\t");
            if (splitted.length == 3 && index != -1){
                context.write(new Text (splitted[index]), line);
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }
    public static class MapperClassNrFile extends Mapper<LongWritable, Text,Text, Text> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
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
        private boolean foundNr = false;
        private long Nr;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            Nr = 0;
            LinkedList<String> list = new LinkedList<>();
            for (Text value : values) {
                String[] splitted = value.toString().split("\t");
                if (splitted.length == 1) {
                    Nr = Long.parseLong(splitted[0]);
                    foundNr = true;
                }
                else if (splitted.length == 3) {
                    if (foundNr) {
                        context.write(new Text(splitted[0]), new Text("" + Nr));
                    } else {
                        list.add(splitted[0]);
                    }
                }
            }

            while (!list.isEmpty()){
                context.write(new Text(list.getFirst()),new Text("" + Nr));
                list.removeFirst();
            }
            foundNr = false;
            Nr = 0;
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
    public static class PartitionerClass extends Partitioner<Text,Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
        }

    }
}
