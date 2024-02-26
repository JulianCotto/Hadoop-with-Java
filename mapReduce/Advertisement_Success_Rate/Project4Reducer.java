//***************************************************************
//
//  Developer:    Julian Cotto
//
//  Project #:    Project Four
//
//  File Name:    Project4R.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     02/25/2024
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  Reducer Project File for MapReduce
//
//***************************************************************
// Project4Reducer.java
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

class Project4Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		//***************************************************************
	    //
	    //  Method:       reducer
	    // 
	    //  Description:  The main method of the program
	    //
	    //  Parameters:   Text, Double, Context
	    //
	    //  Returns:      N/A 
	    //
	    //**************************************************************
		try {
			double sum = 0;
			int count = 0;
			Iterator<DoubleWritable> iter = values.iterator();
			while (iter.hasNext()) {
				DoubleWritable value = iter.next();
				sum += value.get();
				count++;
			}
			// Calculate average
			double average = sum / count;
			// Write the key and average to the context
			context.write(key, new DoubleWritable(average));
		} catch (Exception e) {
			// Log the exception
			System.err.println("Error processing '" + key + "' : " + e.getMessage());
		}
	}
}
