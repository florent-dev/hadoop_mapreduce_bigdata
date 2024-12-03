import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StringAndIntWritableComparable implements WritableComparable<StringAndIntWritableComparable> {
	Text str;
	int count;

	public StringAndIntWritableComparable() {
		this.str = new Text();
		this.count = 0;
	}
	
	public StringAndIntWritableComparable(String str, int count) {
		this.str = new Text(str);
        this.count = count;
    }

	@Override
	public void readFields(DataInput in) throws IOException {
		str.readFields(in);
		count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		str.write(out);
        out.writeInt(count);
	}
	
	@Override
    public int compareTo(StringAndIntWritableComparable o) {
		int cmp = str.compareTo(o.str);
		if (cmp != 0) {
			return cmp;
		}
		return Integer.compare(o.count, count);
    }

	public Text getStr() {
		return str;
	}

	public void setStr(Text str) {
		this.str = str;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	@Override
	public String toString() {
	    return str.toString() + "\t" + count;
	}
}
