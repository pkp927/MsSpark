package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.utils.Vector3D;

public class ConvertToComArray implements Function<Double[], Double[]>{

	// file format 
	Broadcast<Integer> offset;
	Broadcast<Integer> pos;
	Broadcast<Integer> mass_index;
	
	public ConvertToComArray(Broadcast<Integer> of, Broadcast<Integer> p, Broadcast<Integer> mi){
		offset = of;
		pos = p;
		mass_index = mi;
	}
	
	public Double[] call(Double[] d) throws Exception {
		Double[] r = new Double[4];
		int p = pos.value() - offset.value();
		int m = mass_index.value() - offset.value();
		r[0] = d[p]*d[m];
		r[1] = d[p+1]*d[m];
		r[2] = d[p+2]*d[m];
		r[3] = d[m];
		return r;
	}
}