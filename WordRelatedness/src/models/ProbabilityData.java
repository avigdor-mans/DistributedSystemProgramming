package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProbabilityData implements WritableComparable<ProbabilityData>
{
	private Text probType;
	private DoubleWritable probValue;

	public ProbabilityData()
	{
		clear();
	}
	
	public ProbabilityData (String type, double value)
	{
		this.probType = new Text(type);
		this.probValue = new DoubleWritable(value);
	}
	
	private void clear()
	{
		this.probType = new Text();
		this.probValue = new DoubleWritable();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		clear();
		probType.readFields(in);
		probValue.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		probType.write(out);
		probValue.write(out);
	}

	@Override
	public int compareTo(ProbabilityData o)
	{
		int result = this.probType.compareTo(o.probType);
		if (result == 0)
		{
			result = -1 * this.probValue.compareTo(o.probValue); 
		}
		return result;
	}
	
	@Override
	public String toString()
	{
		return 	probType + "\t" + probValue;
	}
	
	public String getProbType()
	{
		return probType.toString();
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