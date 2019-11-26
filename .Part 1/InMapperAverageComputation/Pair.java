import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class Pair implements Writable {
	private int count;
	private int value;

	public Pair(int value, int count) {
		this.count = count;
		this.value = value;
	}
	
	public Pair() {
		this.count = 0;
		this.value = 0;		
	}
	
	@Override
	public String toString() {
		return "( " + count + ", " + value + " )";
	}

	public int getCount() {
		return count;
	}

	public int getValue() {
		return value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		value = in.readInt();
		count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(value);
		out.writeInt(count);
	}

}
