package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.util.RawValue;
import com.google.common.collect.Iterables;

import scala.Tuple2;

public class Main {
	

	public static void main(String[] args) {
		//Input "big" data
		//using a regular java collection
		List<Double> inputData = new ArrayList<Double>();  //equal to: List<Double> inputData = new ArrayList<>();
		inputData.add(35.5);
		inputData.add(12.49943);
		inputData.add(90.32);
		inputData.add(20.32);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);  //org.apache" filter out all the apache library related logging
		//WARN : only shows Warning
		
		SparkConf conf= new SparkConf().setAppName("startingSpark").setMaster("local[*]");  //represent configuration of spark,
		//setMaster("local[*]") means that we use spark in a local configuration without clusters, 
		// * means that use all the avaliable cores on the machine to run, otherwise spark will only use one thread
		
		JavaSparkContext sc= new JavaSparkContext(conf);
		//represent a connection to our spark cluster, this allow us to communicate with spark
		
		//load some kind of data into spark
		JavaRDD<Double> myRdd = sc.parallelize(inputData);  // lead a collection and turn it into an RDD (implemented by Scala)
		//JavaRDD is the java representation of RDDs, which allows us to communicate with RDDs using regular Java syntax without caring out Scala
		//<Double> tell JavaRDD what type of RDD it will handle
		
		
		// Reduces on RDD
		// transform big dataset into a single answer 
		// in real situation, data (RDD) spread in many separately physical nodes
		// define a reduce function
		//example: add reduce
		Double result1 = myRdd.reduce(  (Double value1,Double value2) -> value1 + value2 ) ;  //argument: reduce function to be used
		System.out.println(result1);
		
		//mapping operation
		//transform the structure of RDD from one form to another (create a new RDD)
		JavaRDD<Double> sqrtRdd = myRdd.map( (Double value) -> Math.sqrt( value));
		
		// output the results to the console
		// write the results into a file is the right
		// for test this simple example, output the results to the console
		sqrtRdd.foreach(value -> System.out.println (value));  // the void function will be called on each element of the RDD
		// euqal to : sqrtRdd.foreach( System.out::println  );
		
		//example : counting big data items
		
		// one solution
		long number1 = sqrtRdd.count();
		System.out.println(number1);
		// works only if this is the end step and we want get the answer at java level
		// not works if we want keep the value in the RDD
		
		//another solution:
		//map all the elements into 1, then reduce all the values
		JavaRDD<Long> RDD1 = sqrtRdd.map( value -> 1L ); //1L = new Long(1) ; long can handle bigger data then Integer
		Long number2 = RDD1.reduce( (value1,value2) -> value1 +value2  );
		System.out.println(number2); 
		
		
		// Tuples (comes from scala) : combine data together
		// motivating example, combine RDD and sqrtRDD
		JavaRDD<Tuple2<Double, Double>> RDD_Tuple = myRdd.map( (Double value) -> new Tuple2<>(value,Math.sqrt(value)));
		RDD_Tuple.foreach(value -> System.out.println (value));
		
		
		
		
		//pairRDDs : store key-value
		List<String> inputData1 = new ArrayList<>();
		inputData1.add("WARN: Tuesday 4 September 0405");
		inputData1.add("ERROR: Tuesday 4 September 0408");
		inputData1.add("FATAL: Wednesday 5 September 1632");
		inputData1.add("ERROR: Friday 7 September 1854");
		inputData1.add("WARN: Saturday 8 September 1942");
		
		JavaRDD<String> originalLogMessages = sc.parallelize(inputData1);
		JavaPairRDD<String, String> pairRDD =   originalLogMessages.mapToPair(RawValue -> { 
			String[] column = RawValue.split(":");
			String level = column[0];
			String data = column[1];
			return new Tuple2<String,String>(level,data);
		}   );
		
		originalLogMessages.foreach(value -> System.out.println (value));
		System.out.println (" ");
		pairRDD.foreach(value -> System.out.println (value));
	
		    //coding with a " reduce by Key "
		System.out.println (" ");
		JavaPairRDD<String, Long> pairRDD2 =   originalLogMessages.mapToPair(RawValue -> { 
			String[] column = RawValue.split(":");
			String level = column[0];
			return new Tuple2<String,Long>(level,1L);
		}   );
		JavaPairRDD<String, Long> sumsRdd = pairRDD2.reduceByKey((value1,value2) -> value1+value2);
		sumsRdd.foreach(tuple -> System.out.println(tuple._1 + "has" + tuple._2 + "instances." ));
		
			//codes above is too cumbersome, no need to write so many lines of codes.---- use fluent API
		System.out.println (" ");
		sc.parallelize(inputData1)
		.mapToPair(rawvalue -> new Tuple2<>(rawvalue.split(":")[0],1L) )
		.reduceByKey((value1,value2) -> value1+value2)
		.foreach(tuple -> System.out.println(tuple._1 + "has" + tuple._2 + "instances." ));
		
			//group by key . may crush when dealing with very big data ---- not suggested
		System.out.println (" ");
		sc.parallelize(inputData1)
		.mapToPair(rawvalue -> new Tuple2<>(rawvalue.split(":")[0],1L) )
		.groupByKey()
		.foreach(tuple -> System.out.println(tuple._1 + "has" +Iterables.size(tuple._2) + "instances." ));		
		
		
		
		
		//flat map. overcomes restriction of map: must be one input element with one element output
		JavaRDD<String> senteces =  sc.parallelize(inputData1);
		JavaRDD<String> singleword = senteces.flatMap( value -> Arrays.asList(value.split(" ")).iterator() );
		singleword.foreach( value -> System.out.println (value)  );
		
		//filter: remove values we are not interested in
		//example: filter out numbers
		JavaRDD<String> filteredword = singleword.filter(word -> word.length()>1);
		System.out.println (" ");
		filteredword.foreach( value -> System.out.println (value)  );
		
		
		//read input data from file in the disk
		System.out.println (" ");
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");  // in reality the path is pointed to a distributed file system, not a local path!!
		//remove number and make all letters into lowercase 
		JavaRDD<String> lettersonlyRDD = initialRdd.map(sentence->sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase()); // \s denotes space
		//remove blankline
		JavaRDD<String> noblanklinesRDD = lettersonlyRDD.filter(sentence -> sentence.trim().length()>0);
		
		JavaRDD<String> onlywordRdd = noblanklinesRDD.flatMap(sentece->Arrays.asList(sentece.split(" ")).iterator())
													 .filter(word->word.length()>0);
		JavaRDD<String> noboringwordRdd = onlywordRdd.filter(word->Util.isNotBoring(word)) ;
		
		
		noboringwordRdd.mapToPair(word-> new Tuple2<String, Long>(word, 1L))
			.reduceByKey((value1,value2)->value1+value2)
			.mapToPair(tuple-> new Tuple2<Long, String>(tuple._2 , tuple._1))  //switch key and value
			.sortByKey(false) // this is sorted by the key, not the value
			.take(10)
			.forEach(value->System.out.println (value));
		
		
		
		
		sc.close(); //close sc
		
	}
	
	private void count(String value){
		if (Util.isNotBoring(value)){
			
			
			
		}
	}



}
