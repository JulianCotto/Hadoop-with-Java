//***************************************************************
//
//  Developer:    Julian Cotto
//
//  Project #:    Project Four
//
//  File Name:    Project4Driver.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     02/25/2024
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  Main Project File for MapReduce
//
//***************************************************************
// Import the necessary packages
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//Define the driver class
public class Project4Driver {

// Define the main method
public static void main(String[] args) throws Exception 
{
	//***************************************************************
    //
    //  Method:       main
    // 
    //  Description:  The main method of the program
    //
    //  Parameters:   String array
    //
    //  Returns:      N/A 
    //
    //**************************************************************
	 if (args.length != 2) {
		   // Print an error message and exit
		   System.err.println("Usage: Project4Driver <input path> <output path>");
		   System.exit(-1);
	 }
	 // Create a configuration object
	 Configuration conf = new Configuration();
	 String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
	 Path input = new Path(files[0]);
	 Path output = new Path(files[1]);
	 // Create a job object
	 Job job = Job.getInstance(conf, "Project4Driver");
	 // Set the jar by class
	 job.setJarByClass(Project4Driver.class);
	 // Set the mapper class
	 job.setMapperClass(Project4Mapper.class);
	 // Set the reducer class
	 job.setReducerClass(Project4Reducer.class);
	 // Set the output key class
	 job.setOutputKeyClass(Text.class);
	 // Set the output value class
	 job.setOutputValueClass(DoubleWritable.class);
	 // Set the input path from the first argument
	 FileInputFormat.addInputPath(job, input);
	 FileOutputFormat.setOutputPath(job, output);
	 // Wait for the job to complete and exit with the appropriate status
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
