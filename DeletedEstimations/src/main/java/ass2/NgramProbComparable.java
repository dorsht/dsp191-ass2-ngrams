package ass2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NgramProbComparable implements WritableComparable<NgramProbComparable> {
        private String[] splittedNgram;
        private Double prob;
        private String line;

        public String[] getSplittedNgram (){
            return this.splittedNgram;
        }
        public Double getProb (){
            return this.prob;
        }

        NgramProbComparable(Text line) {
            this.line = line.toString();
            String[] splitted = line.toString().split("\t");
            this.splittedNgram = splitted[0].split(" ");
            this.prob = Double.parseDouble(splitted[1]);
        }

        // Without any usage, We wrote It only because It's needed by hadoop...
        NgramProbComparable(){

        }


        @Override
        public void write(DataOutput d) throws IOException {
            d.writeUTF(toString());
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            String input = di.readUTF();
            String[] inputByTab = input.split("\t");
            if (inputByTab.length > 1) {
                this.splittedNgram = inputByTab[0].split(" ");
                this.line = inputByTab[0];
                this.prob = Double.parseDouble(inputByTab[1]);
            }
            else{
                this.splittedNgram = new String[0];
                this.line = "";
                this.prob = Double.parseDouble(inputByTab[0]);
            }
        }

        @Override
        public String toString() {
            return this.line+"\t"+this.prob;
        }

        @Override
        public int compareTo(NgramProbComparable otherNgram) {
            String[] splittedOtherNgram = otherNgram.getSplittedNgram();
            Double otherProb = otherNgram.getProb();
            int firstRes = this.splittedNgram[0].compareTo(splittedOtherNgram[0]);
            if (firstRes == 0 && this.splittedNgram.length > 1 && splittedOtherNgram.length > 1){
                int secondRes = this.splittedNgram[1].compareTo(splittedOtherNgram[1]);
                if (secondRes == 0){
                    return otherProb.compareTo(this.prob);
                }
                else return secondRes;
            }
            else return firstRes;

        }
    }




