//***************************************************************
//
//  Developer:     <Your full name>
//
//  Program #:     <Assignment Number>
//
//  File Name:     <TuplesDemo2.java>
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

public class TuplesDemo2
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
		
        developerInfo();
		
		// We don't want to keep these two set of data in two separate RDDs
		JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
		JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalIntegers.map(value -> new IntegerWithSquareRoot(value));

		sqrtRdd.foreach(object -> System.out.println(object.getOriginalNumber() + "  " + object.getSquareRoot()));

		long count = sqrtRdd.count();
        System.out.println(count);
        
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
