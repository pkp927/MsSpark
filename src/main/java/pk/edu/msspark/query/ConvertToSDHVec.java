package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.utils.Vector3D;

public class ConvertToSDHVec implements Function<Double[], Vector3D>{

	// file format 
	Broadcast<Integer> offset;
	Broadcast<Integer> pos;
	
	public ConvertToSDHVec(Broadcast<Integer> of, Broadcast<Integer> p){
		offset = of;
		pos = p;
	}
	
	public Vector3D call(Double[] d) throws Exception {
		Vector3D v = new Vector3D();
		int p = pos.value() - offset.value();
		v.x = d[p];
		v.y = d[p+1];
		v.z = d[p+2];
		return v;
	}

}