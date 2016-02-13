package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function;

/* Class to calculate ROG for mapValues transformation
 */
public class ConvertToRog implements Function<Double[], Double>{

	public Double call(Double[] d) throws Exception {
		return (Math.sqrt(d[0]/d[1]));
	}

}
