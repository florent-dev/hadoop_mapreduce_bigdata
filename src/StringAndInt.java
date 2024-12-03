import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringAndInt implements Comparable<StringAndInt>, Writable {
	Text key = new Text();
	IntWritable value = new IntWritable();

	public StringAndInt() {
		this.key = new Text();
		this.value = new IntWritable();
	}
	
	public StringAndInt(String key, Integer value) {
		this.key = new Text(key);
        this.value = new IntWritable(value);
    }
	
	@Override
	public int compareTo(StringAndInt o) {
		int cmp = key.compareTo(o.key);
		if (cmp != 0) {
			return cmp;
		}
		return value.compareTo(o.value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// for deserialization
		key.readFields(in);
        value.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// for serialization
		key.write(out);
        value.write(out);
	}

	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public IntWritable getValue() {
		return value;
	}

	public void setValue(IntWritable value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
	    return key.toString() + "\t" + value.toString();
	}
}
