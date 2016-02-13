package pk.edu.msspark.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

/* Class to calculate SDH partitionwise for mapPartition function
 */
class CalculateSDHdist implements VoidFunction<Iterator<Tuple2<Integer,Vector3D>>>{
	
	private Accumulator<Map<Integer,String>> acc;
	private Broadcast<Integer> bw;
	
	public CalculateSDHdist(Accumulator<Map<Integer,String>> a, Broadcast<Integer> b){
		acc = a;
		bw = b;
	}
	
	public Double euclideanDistance(Vector3D v1, Vector3D v2){
		Double x = Math.abs(v1.x - v2.x);
		Double y = Math.abs(v1.y - v2.y);
		Double z = Math.abs(v1.z - v2.z);
		return Math.sqrt(x*x+y*y+z*z);
	}
	
	public String printMap(HashMap mp) {
		String h = "";
	    Iterator it = mp.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
	        h = h+"("+pair.getKey()+":"+pair.getValue()+")";
	    }
	    return h;
	}
	
	public void call(Iterator<Tuple2<Integer, Vector3D>> tup) throws Exception {
		ArrayList<Vector3D> pos1 = new ArrayList<Vector3D>();
		ArrayList<Vector3D> pos2 = new ArrayList<Vector3D>();
		Tuple2<Integer, Vector3D> t = tup.next();
		int frame = t._1;
		pos1.add(t._2);
		pos2.add(t._2);
		while(tup.hasNext()){
			t = tup.next();
			pos1.add(t._2);
			pos2.add(t._2);
		}
		
		ArrayList<Double> dist = new ArrayList<Double>();
		Vector3D v1, v2;
		double max = 0.0;
		double d;
		for(int i=0;i<pos1.size();i++){
			v1 = pos1.get(i);
			for(int j=0;j<pos2.size();j++){
				v2 = pos2.get(j);
				d = euclideanDistance(v1, v2);
				dist.add(d);
				if(d>max) max = d;
				//System.out.println(euclideanDistance(v1, v2));
			}
		}
		
		HashMap<Double, Integer> hist = new HashMap<Double, Integer>();
		double bin = 0.0;
		double width = max/3;
		double[] binw = new double[bw.value()+1];
		for(int i=0; i<bw.value(); i++){
			hist.put(bin, 0);
			binw[i] = bin;
			bin = bin+width;
		}
		binw[bw.value()] = bin;
		for(int i=0; i<dist.size(); i++){
			d = dist.get(i);
			for(int j=0;j<bw.value();j++){
				if((d>=binw[j]) && (d<=binw[j+1])){
					hist.put(binw[j], hist.get(binw[j])+1);
				}
			}
		}
		System.out.println(frame);
		Map<Integer, String> m = new HashMap<Integer,String>();
		m.put(frame,printMap(hist));
		acc.add(m);

	}
}