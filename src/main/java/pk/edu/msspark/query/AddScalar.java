package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function2;

/* Class to add two double values of pairRDD for reduceByKey transformation
 */
public class AddScalar implements Function2<Double, Double, Double>{

	public Double call(Double m1, Double m2) throws Exception {
		return (m1+m2);
	}
	
}
