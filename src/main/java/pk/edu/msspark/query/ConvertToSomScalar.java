package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.utils.Vector3D;

class ConvertToSomScalar implements Function<Double[], Double>{

	// file format 
	Broadcast<Integer> offset;
	Broadcast<Integer> mass_index;
	
	public ConvertToSomScalar(Broadcast<Integer> of, Broadcast<Integer> mi){
		offset = of;
		mass_index = mi;
	}
	
	public Double call(Double[] d) throws Exception {
		int m = mass_index.value() - offset.value();
		return d[m];
	}
}