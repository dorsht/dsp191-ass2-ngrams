package ass2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NgramSortComparator extends WritableComparator {

    public NgramSortComparator() {
        super(NgramProbComparable.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        NgramProbComparable firstNgram = (NgramProbComparable) first;
        NgramProbComparable secondNgram = (NgramProbComparable) second;
        return firstNgram.compareTo(secondNgram);
    }

}
