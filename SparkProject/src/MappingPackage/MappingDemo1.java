//***************************************************************
//
//  Developer:     <Your full name>
//
//  Program #:     <Assignment Number>
//
//  File Name:     <MappingDemo1.java>
//
//  Course:        COSC-3365 Distributed Databases using Hadoop
//
//  Due Date:      <Due Date>
//
//  Instructor:    Fred Kumi 
//
//  Hadoop Topic:  Apache Spark
//
//  Description:   <An explanation of what the program is designed to do>
//
//***************************************************************

package MappingPackage;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MappingDemo1
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
	public static void main(String[] args) 
	{
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49);
        inputData.add(90.12);
        inputData.add(20.32);
        inputData.add(15.5);
        inputData.add(25.2);
        
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        
		developerInfo();
		
        // Set App name (.setAppName)  configure for local (.setMaster("local[*]"))
        SparkConf conf = new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
        
        // JavaSparkContext - Represent a connection to our Spark cluster
        //  We don't have a Cluster yet so the object will let us communicate with Spark 
        JavaSparkContext sc = new JavaSparkContext(conf);  // Working with Spark now
        
        // Now let's load some data into Spark
        // The data can be from Hadoop HDFS
        // For now we are using local input data, the ArrayList, inputData
        // Use the parallelize method of SparkContext to load the data into an RDD
        // JavaRDD is a Java representation of Spark RDD written in Scala
        // Under the hood, JavaRDD is communicating with a Scala RDD
        // JavaRDD is a wrapper class of the Scala RDD
        JavaRDD<Double> myRdd = sc.parallelize(inputData);
        
        // Now it us perform an operation against our RDD 
        // This is a reduce method that accepts a lambda expression
        Double results = myRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println("Results = " + results);
        
        // The map method lets us transform the structure of one RDD to another
        JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));
        
        // Print using Method Reference
        sqrtRdd.collect().forEach(System.out::println);
        
		// Count how many elements in sqrtRdd using just map and reduce
        System.out.println(sqrtRdd.count());
              
        sc.close();
	}
		
	//***************************************************************
    //
    //  Method:       developerInfo
    // 
    //  Description:  The developer information method of the program
    //
    //  Parameters:   None
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    public static void developerInfo()
    {
       System.out.println("Name:    <Put your full name here>");
       System.out.println("Course:  COSC-3365 Distributed Databases using Hadoop");
       System.out.println("Program: XXX \n");

    } // End of developerInfo
}
