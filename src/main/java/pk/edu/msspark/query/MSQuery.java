package pk.edu.msspark.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.resultRDD.*;
import pk.edu.msspark.selectionRDD.FrameAtomsRDD;
import pk.edu.msspark.selectionRDD.SelectedAtomsRDD;
import pk.edu.msspark.utils.*;
import scala.Tuple2;

/* MSQuery class implements MS queries
 * for Molecular Simulation data
 */

public class MSQuery{

	// file format 
	Broadcast<Integer> offset;
	Broadcast<Integer> pos;
	Broadcast<Integer> mass_index;
	Broadcast<Integer> charge_index;
	// spark context object
	JavaSparkContext sc;
	
	/* Constructor that broadcasts the required variables
	 */
	public MSQuery(JavaSparkContext sc){
		this.sc = sc;
		offset = sc.broadcast(FileFormat.FRAME_NO_INDEX + 1);
		pos = sc.broadcast(FileFormat.POS_VEC_INDEX);
		mass_index = sc.broadcast(FileFormat.MASS_INDEX);
		charge_index = sc.broadcast(FileFormat.CHARGE_INDEX);
	}
	
	/* Function to calculate MOI 
	 */
	public VectorRDD getMOI(FrameAtomsRDD f){		
		return new VectorRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToMoiVec(offset, pos, mass_index))
				.reduceByKey(new AddVector()));
	}
	
	/* Function to calculate MOI 
	 */
	public VectorRDD getMOI(SelectedAtomsRDD f){
		return new VectorRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToMoiVec(offset, pos, mass_index))
				.reduceByKey(new AddVector()));
	}
	
	/* Function to calculate SOM 
	 */
	public ScalarRDD getSOM(FrameAtomsRDD f){		
		return new ScalarRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToSomScalar(offset, mass_index))
				.reduceByKey(new AddScalar()));
	}
	
	/* Function to calculate SOM 
	 */
	public ScalarRDD getSOM(SelectedAtomsRDD f){
		return new ScalarRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToSomScalar(offset, mass_index))
				.reduceByKey(new AddScalar()));
	}
	
	/* Function to calculate COM 
	 */
	public VectorRDD getCOM(FrameAtomsRDD f){		
		return new VectorRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToComArray(offset, pos, mass_index))
				.reduceByKey(new AddArray())
				.mapValues(new ConvertToCom()));
	}
	
	/* Function to calculate COM 
	 */
	public VectorRDD getCOM(SelectedAtomsRDD f){
		return new VectorRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToComArray(offset, pos, mass_index))
				.reduceByKey(new AddArray())
				.mapValues(new ConvertToCom()));
	}
	
	/* Function to calculate DM
	 */
	public VectorRDD getDM(FrameAtomsRDD f){		
		return new VectorRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToDMVec(offset, pos, charge_index))
				.reduceByKey(new AddVector()));
	}
	
	/* Function to calculate DM 
	 */
	public VectorRDD getDM(SelectedAtomsRDD f){
		return new VectorRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToDMVec(offset, pos, charge_index))
				.reduceByKey(new AddVector()));
	}
	
	/* Function to calculate ROG
	 */
	public ScalarRDD getROG(FrameAtomsRDD f, String ax){
		int a;
		if(ax.equals("x")) a = 0;
		else if(ax.equals("y")) a = 1;
		else a = 2;	
		Broadcast<Integer> axis = sc.broadcast(a);
		return new ScalarRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToRogArray(offset, pos, mass_index, axis))
				.reduceByKey(new AddArray())
				.mapValues(new ConvertToRog()));
	}
	
	/* Function to calculate ROG 
	 */
	public ScalarRDD getROG(SelectedAtomsRDD f, String ax){
		int a;
		if(ax.equals("x")) a = 0;
		else if(ax.equals("y")) a = 1;
		else a = 2;
		Broadcast<Integer> axis = sc.broadcast(a);
		return new ScalarRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToRogArray(offset, pos, mass_index, axis))
				.reduceByKey(new AddArray())
				.mapValues(new ConvertToRog()));
	}
	
	/* Function to calculate SDH
	 */
	public HistogramRDD getSDH( FrameAtomsRDD f, int[] frames, int bw){		
		Broadcast<Integer> binw = sc.broadcast(bw);
		JavaPairRDD<Integer, Vector3D> res = f.getFrameAtomsRDD()
				.mapValues(new ConvertToSDHVec(offset, pos));
		JavaPairRDD<Integer, Iterable<Vector3D>> result = res.groupByKey();
		JavaPairRDD<Integer, String> hist = result.mapValues(new CalculateSDH(binw));
		return new HistogramRDD(hist);
	}
	
	/* Function to calculate SDH
	 */
	public HistogramRDD getSDH(SelectedAtomsRDD f, int[] frames, int bw){
		Broadcast<Integer> binw = sc.broadcast(bw);
		JavaPairRDD<Integer, Vector3D> res = f.getSelectedAtomsRDD()
				.mapValues(new ConvertToSDHVec(offset, pos));
		JavaPairRDD<Integer, Iterable<Vector3D>> result = res.groupByKey();
		JavaPairRDD<Integer, String> hist = result.mapValues(new CalculateSDH(binw));
		return new HistogramRDD(hist);
		
		/*result.foreachPartition(new CalculateSDHdist(acc, binw));
		Map<Integer, String> mp = acc.value();
		Iterator it = mp.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
	    }*/
		/*
		result.cache();
		Broadcast<Integer> frame;
		JavaRDD<Vector3D> position;
		JavaDoubleRDD dist;
		Tuple2<double[], long[]> hist;
		String h;
		ArrayList<Tuple2<Integer,String>> output = new ArrayList<Tuple2<Integer,String>>();
		for(int i=0;i<frames.length;i++){
			frame = sc.broadcast(frames[i]);
			position =result.
					filter(new GetFrame(frame)).values();
			dist =	position.cartesian(position).map(new CalculateDist()).mapToDouble(new GetDouble());
			hist = dist.histogram(bw);
			System.out.println("Frame no: "+frames[i]);
			h = "";
			for(int j=0;(j<hist._1.length)&&(j<hist._2.length);j++){
				System.out.println(hist._1[j]+" : "+hist._2[j]);
				h = h+"("+hist._1[j]+" : "+hist._2[j]+")";
			}
			output.add(new Tuple2<Integer,String>(frames[i],h));
		}
		return new HistogramRDD(sc, output);
		*/
	}
	
	
	
}



