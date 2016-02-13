package pk.edu.msspark.query;

import org.apache.spark.api.java.function.DoubleFunction;

/* Class for mapToDouble transformation
 */
class GetDouble implements DoubleFunction<Double>{

		public double call(Double d) throws Exception {
			return d;
		}
		
	}