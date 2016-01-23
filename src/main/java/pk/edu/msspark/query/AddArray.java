package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function2;

import pk.edu.msspark.utils.Vector3D;

public class AddArray implements Function2<Double[], Double[], Double[]>{

	public Double[] call(Double[] d1, Double[] d2) throws Exception {
		//int len = (d1.length<d2.length) ? d1.length : d2.length ;
		int len = d1.length;
		Double[] d = new Double[len];
		for(int i=0; i<len; i++){
			d[i] = d1[i]+d2[i];
		}
		return d;
	}
	
}