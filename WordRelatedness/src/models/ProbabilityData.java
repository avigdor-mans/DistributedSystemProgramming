package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProbabilityData implements WritableComparable<ProbabilityData>
{
	private Text ProbTybe;
	private DoubleWritable probValue;

	public ProbabilityData()
	{
		clear();
	}
	
	public ProbabilityData (String type, double value)
	{
		this.ProbTybe = new Text(type);
		this.probValue = new DoubleWritable(value);
	}
	
	private void clear()
	{
		this.ProbTybe = new Text();
		this.probValue = new DoubleWritable();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		clear();
		ProbTybe.readFields(in);
		probValue.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		ProbTybe.write(out);
		probValue.write(out);
	}

	@Override
	public int compareTo(ProbabilityData o)
	{
		int result = this.ProbTybe.compareTo(o.ProbTybe);
		if (result == 0)
		{
			result = -1 * this.probValue.compareTo(o.probValue); 
		}
		return result;
	}
	
	@Override
	public String toString()
	{
		return 	ProbTybe + "\t" + probValue;
	}
	
	public String getProbType()
	{
		return ProbTybe.toString();
	}
	
	public double getProbValue() 
	{
		return probValue.get();
	}

	public void setProbValue(double probVal)
	{
		this.probValue = new DoubleWritable(probVal);
	}

}