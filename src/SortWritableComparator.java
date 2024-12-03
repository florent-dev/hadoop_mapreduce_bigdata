import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortWritableComparator extends WritableComparator {
	public SortWritableComparator() {
		super(StringAndIntWritableComparable.class, true);
	}
	
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		StringAndIntWritableComparable s1 = (StringAndIntWritableComparable) wc1;
		StringAndIntWritableComparable s2 = (StringAndIntWritableComparable) wc2;
		return s1.compareTo(s2);
	}

}
