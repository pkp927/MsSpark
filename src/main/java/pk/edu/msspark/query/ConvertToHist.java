package pk.edu.msspark.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

public class ConvertToHist implements Serializable,PairFlatMapFunction<Iterator<Tuple2<Integer,Vector3D>>,Integer,Tuple2<double[],long[]>>{

	JavaSparkContext sc;
	Broadcast<Integer> bin;
	JavaDoubleRDD dist;
	
	public ConvertToHist(JavaSparkContext sc, Broadcast<Integer> bin){
		this.sc = sc;
		this.bin = bin;
	}
	
	public Iterable<Tuple2<Integer, Tuple2<double[], long[]>>> call(Iterator<Tuple2<Integer, Vector3D>> input) throws Exception {
		ArrayList<Double> res = new ArrayList<Double>();
		ArrayList<Vector3D> pos = new ArrayList<Vector3D>();
		
		Tuple2<Integer, Vector3D> t = input.next();
		Integer f = t._1;
		pos.add(t._2);
		while(input.hasNext()){
			pos.add(input.next()._2);		
		}
		
		Vector3D v1,v2;
		for(int i=0;i<pos.size();i++){
			v1 = pos.get(i);
			for(int j=0;j<pos.size();j++){
				v2 = pos.get(j);
				res.add(euclideanDistance(v1, v2));
			}
		}
		
		
		dist = sc.parallelizeDoubles(res);
		
		Tuple2<Integer, Tuple2<double[], long[]>> result = new Tuple2<Integer, 
				Tuple2<double[], long[]>>(f, dist.histogram(bin.value()));
		
		return (Iterable<Tuple2<Integer, Tuple2<double[], long[]>>>) result;
	}
	
	public Double euclideanDistance(Vector3D v1, Vector3D v2){
		Double x = Math.abs(v1.x - v2.x);
		Double y = Math.abs(v1.y - v2.y);
		Double z = Math.abs(v1.z - v2.z);
		return Math.sqrt(x*x+y*y+z*z);
	}


}