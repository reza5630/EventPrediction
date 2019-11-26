import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	private Text left;
	private Text right;

	public Pair(Text value, Text count) {
		this.left = value;
		this.right = count;
	}

	public Pair() {
		this.left = new Text("");
		this.right = new Text("");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((left == null) ? 0 : left.hashCode());
		result = prime * result + ((right == null) ? 0 : right.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (left == null) {
			if (other.left != null)
				return false;
		} else if (!left.equals(other.left))
			return false;
		if (right == null) {
			if (other.right != null)
				return false;
		} else if (!right.equals(other.right))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "( " + left + ", " + right + " )";
	}

	public Text getLeft() {
		return left;
	}

	public Text getRight() {
		return right;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		left.readFields(in);
		right.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		left.write(out);
		right.write(out);
	}

	@Override
	public int compareTo(Pair o) {
		// TODO Auto-generated method stub
		if (o == null)
			return 0;
		if (this.getLeft().compareTo(o.getLeft()) != 0)
			return this.getLeft().compareTo(o.getLeft());
		else
			return this.getRight().compareTo(o.getRight());
	}

}
