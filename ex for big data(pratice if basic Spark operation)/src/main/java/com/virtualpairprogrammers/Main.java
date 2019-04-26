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

import com.virtualpairprogrammers.ViewingFigures;


public class Main {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN); 
		SparkConf conf= new SparkConf().setAppName("startingSpark").setMaster("local[*]");  
		JavaSparkContext sc= new JavaSparkContext(conf);
		
		boolean testMode = true;
		
		JavaPairRDD<Integer, Integer> viewData = 
		
		
		JavaRDD<Double> myRdd = sc.parallelize(inputData);  

		
		ViewingFigures.
	

		Double result1 = myRdd.reduce(  (Double value1,Double value2) -> value1 + value2 ) ;  //argument: reduce function to be used
		System.out.println(result1);
		
		//mapping operation
		//transform the structure of RDD from one form to another (create a new RDD)
		JavaRDD<Double> sqrtRdd = myRdd.map( (Double value) -> Math.sqrt( value));
		
		
	}

}
