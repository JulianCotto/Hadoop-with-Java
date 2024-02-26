//***************************************************************
//
//  Developer:    Julian Cotto
//
//  Project #:    Project Four
//
//  File Name:    Project4Mapper.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     02/25/2024
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  Mapper Project File for MapReduce
//
//***************************************************************
// Project4Mapper.java
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class Project4Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//***************************************************************
	    //
	    //  Method:       mapper
	    // 
	    //  Description:  The mapper method of the program
	    //
	    //  Parameters:   Long, Text, Context
	    //
	    //  Returns:      N/A 
	    //
	    //**************************************************************
		try {
			// Convert the line to string
			String line = value.toString();
			// Split the line into words
			String[] words = line.split(",");
			for (String word : words) {
				// Extract category and location from the words
				String category = words[3];
				String location = words[2];
				// Parse clicks and sales from the words
				double clicks = Integer.parseInt(words[4]);
				double sales = Integer.parseInt(words[5]);
				// Calculate success rate
				double successRate = sales / clicks * 100;
				// Write the category, location and success rate to the context
				context.write(new Text(category + " " + location), new DoubleWritable(successRate));
			}
		} catch (Exception e) {
			// Log the exception
			System.err.println("Error processing '" + value + "' : " + e.getMessage());
		}
	}
}
