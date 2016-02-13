package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

/* Class to extract ROG required attributes for mapValues transformation
 */
public class ConvertToRogArray implements Function<Double[], Double[]>{

	// file format 
	Broadcast<Integer> offset;
	Broadcast<Integer> pos;
	Broadcast<Integer> mass_index;
	Broadcast<Integer> axis;
	
	public ConvertToRogArray(Broadcast<Integer> of, Broadcast<Integer> p, Broadcast<Integer> mi, Broadcast<Integer> a){
		offset = of;
		pos = p;
		mass_index = mi;
		axis = a;
	}
	
	public Double[] call(Double[] d) throws Exception {
		Double[] r = new Double[2];
		int p = pos.value() - offset.value() + axis.value();
		int m = mass_index.value() - offset.value();
		r[0] = d[p]*d[m];
		r[1] = d[m];
		return r;
	}
}