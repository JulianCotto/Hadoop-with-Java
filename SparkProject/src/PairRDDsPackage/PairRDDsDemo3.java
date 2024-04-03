package PairRDDsPackage;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class PairRDDsDemo3
{
	// @SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		List<String> inputData = new ArrayList<>();
		
		inputData.add("WARN: Saturday, March 5, 2022 0405");
		inputData.add("ERROR: Saturday, March 5, 2022 0508");
		inputData.add("FATAL: Sunday, March 6, 2022 1632");
		inputData.add("ERROR: Sunday, March 6, 2022 1854");
		inputData.add("WARN: Monday, March 7, 2022 0945");
		inputData.add("WARN: Monday, March 7, 2022 1942");
		inputData.add("FATAL: Tuesday, March 8, 2022 2042");
		inputData.add("WARN: Tuesday, March 8, 2022 1002");
		inputData.add("ERROR: Wednesday, March 9, 2022 1205");
		inputData.add("FATAL: Wednesday, March 9, 2022 1542");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> originalLogData = sc.parallelize(inputData);
		
		JavaPairRDD<String, Long> myPairRDD = originalLogData.mapToPair(rawData -> {
			String[] columns = rawData.split(":");
			String errorLevel = columns[0];
			
			// return new Tuple2<String, String>(errorLevel, dateTime);
			return new Tuple2<>(errorLevel, 1L);
		});

		myPairRDD.collect().forEach(System.out::println);
  
		sc.close();
	}
}
