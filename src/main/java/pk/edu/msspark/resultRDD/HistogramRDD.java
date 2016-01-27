package pk.edu.msspark.resultRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/* HistogramRDDRDD class to represent scator data type
 * and operations on this data type
 */
public class HistogramRDD implements Serializable{

    // frame number is mapped to scator quantity
	JavaPairRDD<Integer, String> hist;

    // default constructor
	public HistogramRDD(){
		this.hist = null;
	}

    // constructor
	public HistogramRDD(JavaPairRDD<Integer,String> hist){
		this.hist = hist;
	}
	
	// constructor
	public HistogramRDD(JavaSparkContext sc, ArrayList<Tuple2<Integer,String>> list){
		this.hist = sc.parallelizePairs(list);
	}

    // setter method
	public void setHistogramRDD(JavaPairRDD<Integer,String> hist){
		this.hist = hist;
	}

    // getter method
	public JavaPairRDD<Integer,String> getHistogramRDD(){
		return this.hist;
	}
	
	// function to print the data
	public void printHistogram(){
		
	}
	
}

