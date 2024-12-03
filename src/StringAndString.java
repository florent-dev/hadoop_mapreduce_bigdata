import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StringAndString implements WritableComparable<StringAndString> {
	Text country = new Text();
	Text tag = new Text();
	
	public StringAndString() {
        this.country.set("");
        this.tag.set("");
    }
	
	public StringAndString(Text country, Text tag) {
		super();
        this.country.set(country);
        this.tag.set(tag);
    }
	
	@Override
	public int compareTo(StringAndString o) {
        int res = this.country.compareTo(o.country);
        if (res != 0) {
        	return res;
        }
        return this.tag.compareTo(o.tag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// for deserialization
		this.country.readFields(in);
        this.tag.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// for serialization
		this.country.write(out);
        this.tag.write(out);
	}

	public Text getCountry() {
		return country;
	}

	public void setCountry(Text country) {
		this.country = country;
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = tag;
	}

    @Override
    public String toString() {
        return country + "#" + tag;
    }
	
}
