//***************************************************************
//
//  Developer:     <Your full name>
//
//  Program #:     <Assignment Number>
//
//  File Name:     <TuplesDemo1.java>
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

package TuplesPackage;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TuplesDemo1
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
	// @SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(16);
		inputData.add(20);
		inputData.add(45);
		inputData.add(70);
		inputData.add(40);
		inputData.add(9);
		inputData.add(100);
		inputData.add(81);
		inputData.add(28);
		inputData.add(48);
		inputData.add(76);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);

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
		JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
		
        // Now it us perform an operation against our RDD 
        // This is a reduce method that accepts a lambda expression
        // The map method lets us transform the structure of one RDD to another
		JavaRDD<Double> sqrtRdd = originalIntegers.map(value -> Math.sqrt(value));

        developerInfo();

        // Method reference
        sqrtRdd.collect().forEach(System.out::println);
        sqrtRdd.collect().forEach(value -> System.out.println(value));
		
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
