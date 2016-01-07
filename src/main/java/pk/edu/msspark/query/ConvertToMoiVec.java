package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.utils.Vector3D;

class ConvertToMOIVec implements Function<Double[], Vector3D>{

	// file format 
	Broadcast<Integer> offset;
	Broadcast<Integer> pos;
	Broadcast<Integer> mass_index;
	
	public ConvertToMOIVec(Broadcast<Integer> of, Broadcast<Integer> p, Broadcast<Integer> mi){
		offset = of;
		pos = p;
		mass_index = mi;
	}
	
	public Vector3D call(Double[] d) throws Exception {
		Vector3D v = new Vector3D();
		int p = pos.value() - offset.value();
		int m = mass_index.value() - offset.value();
		v.x = d[p]*d[m];
		v.y = d[p+1]*d[m];
		v.z = d[p+2]*d[m];
		return v;
	}
}