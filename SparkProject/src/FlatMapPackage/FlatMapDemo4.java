package FlatMapPackage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FlatMapDemo4
{
	//  @SuppressWarnings("resource")
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

		sc.parallelize(inputData)
		  .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
		  .filter(word -> word.length() > 3)
		  .collect()
		  .forEach(System.out::println);

		sc.close();
	}
}
