package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function2;

import pk.edu.msspark.utils.Vector3D;

public class AddScalar implements Function2<Double, Double, Double>{

	public Double call(Double m1, Double m2) throws Exception {
		return (m1+m2);
	}
	
}
