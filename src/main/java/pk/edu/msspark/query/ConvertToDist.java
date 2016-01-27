package pk.edu.msspark.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

public class ConvertToDist implements FlatMapFunction<Iterator<Vector3D>,Double>{

	
	public Double euclideanDistance(Vector3D v1, Vector3D v2){
		Double x = Math.abs(v1.x - v2.x);
		Double y = Math.abs(v1.y - v2.y);
		Double z = Math.abs(v1.z - v2.z);
		return Math.sqrt(x*x+y*y+z*z);
	}

	public Iterable<Double> call(Iterator<Vector3D> input) throws Exception {
		ArrayList<Double> result = new ArrayList<Double>();
		ArrayList<Vector3D> pos = new ArrayList<Vector3D>();
		
		while(input.hasNext()){
			pos.add(input.next());		
		}
		Vector3D v1,v2;
		for(int i=0;i<pos.size();i++){
			v1 = pos.get(i);
			for(int j=0;j<pos.size();j++){
				v2 = pos.get(j);
				result.add(euclideanDistance(v1, v2));
			}
		}
		
		return result;
	}


}