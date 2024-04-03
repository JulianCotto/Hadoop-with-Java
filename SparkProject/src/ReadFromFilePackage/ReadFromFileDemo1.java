package ReadFromFilePackage;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class ReadFromFileDemo1
{
	// @SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		conf.set("spark.driver.bindAddress", "127.0.0.1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> originalLogData = sc.textFile("InputFile.txt");

		JavaPairRDD<String, Long> myPairRDD = originalLogData.mapToPair(rawData -> {
			String[] columns = rawData.split(":");
			String errorLevel = columns[0];
			
			// return new Tuple2<String, String>(errorLevel, dateTime);
			return new Tuple2<>(errorLevel, 1L);
		});
		
		JavaPairRDD<String, Long> sumsRdd = myPairRDD.reduceByKey((value1, value2) -> value1 + value2);
 
        sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
  
		sc.close();
	}
}
