package pk.edu.msspark.app;
/* SimpleApp.java */

import org.apache.spark.api.java.*;

import java.io.File;
import java.io.Serializable;
import java.util.Scanner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;

import pk.edu.msspark.query.OneBodyQuery;
import pk.edu.msspark.resultRDD.VectorRDD;
import pk.edu.msspark.selectionRDD.FrameAtomsRDD;
import pk.edu.msspark.selectionRDD.SelectedAtomsRDD;
import pk.edu.msspark.utils.SelectParameters;
import pk.edu.msspark.utils.Vector3D;
import pk.edu.msspark.cache.*;
import scala.Tuple2;

/* MSApp class is the main class to get the input files 
 * and generate output files
 */

public class MSApp {
	
	  public static void main(String[] args) {

	    // create SparkContext object to access clusters
	    SparkConf conf = new SparkConf().setAppName("Simple Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

	    // specify input and output location of files
	    //String inputLoc = "hdfs://localhost:54310/example/data.txt";
	    //String outputLoc = "hdfs://localhost:54310/example/output";
	    String inputLoc = "/home/simi/Desktop/thesis/data.txt";
	    String outputLoc = "/home/simi/spark-1.5.1/abc/output";
	    String cacheLoc = "/home/simi/spark-1.5.1/abc/cache";
	    
	    File f = new File(outputLoc);
	    if(f.exists()){
		    String[] entries = f.list();
		    for(String s: entries){
		    	File currentFile = new File(f.getPath(),s);
		    	currentFile.delete();
		    }
		    f.delete();
	    }
	    // set parameter variable;
	    SelectParameters param = new SelectParameters();
	    
	    // create FrameAtomsRDD out of input data
	    FrameAtomsRDD distData = new FrameAtomsRDD(sc, inputLoc);
	    //distData.getFrameAtomsRDD().persist(StorageLevel.MEMORY_AND_DISK());
	    
	    // testing
	    while(true){
	    	// get input parameters for selection from user
		    Scanner reader = new Scanner(System.in);
		    System.out.println("Enter the first frame no:");
		    String a = reader.nextLine();
		    System.out.println("Enter the last frame no:");
		    String b = reader.nextLine();
		    System.out.println("Enter the skip frame nos:");
		    String c = reader.nextLine();
		    System.out.println("Enter the min boundary:");
		    String d = reader.nextLine();
		    System.out.println("Enter the max boundary:");
		    String e = reader.nextLine();
		    
		    // set the parameters for the selection of desired atoms
		    param.firstFrame = Integer.parseInt(a);  
		    param.lastFrame = Integer.parseInt(b);      
		    param.skip = c;      					
		    String[] split = d.split("\\s+");
		    if(split.length > 1) param.minBound = new Vector3D(
		    		Double.parseDouble(split[0]),Double.parseDouble(split[1]),Double.parseDouble(split[2]));
		    else param.minBound = null;
		    split = e.split("\\s+");
		    if(split.length > 1) param.maxBound = new Vector3D(
		    		Double.parseDouble(split[0]),Double.parseDouble(split[1]),Double.parseDouble(split[2]));  
		    else param.maxBound = null;
		    param.atomType = "";		
		    param.atomId = "";	
		    param.cached = false;	
		    
		    if(param.cached){
		    	param = MoiCache.checkMOIcache(sqlContext, cacheLoc, param);
		    }
	    	
		    if(!param.cached){
			    // get desired data
			    SelectedAtomsRDD selData = new SelectedAtomsRDD(sc, distData, param);
			    //selData.getSelectedAtomsRDD().coalesce(1,true).saveAsTextFile(outputLoc);
			    
			    while(true){
			    	// execute the query -> Moment of Inertia
			    	OneBodyQuery q = new OneBodyQuery(sc);
			    	VectorRDD MOI = q.getMOI(selData);
			    
		        	// save the result in the output folder
		        	//MOI.getVectorRDD().coalesce(1,true).saveAsTextFile(outputLoc);
		        	MOI.getVectorRDD().foreach(new PrintVecTuple());
		        	
			        // cache the result
			        //MoiCache.cacheMOIresult(sqlContext, MOI, cacheLoc, param);
		        	
		        	// check for finish
			    	System.out.println("Do u wanna change the data selection?(n/y):");
			    	String r = reader.nextLine();
			    	if(r.equals("y")){
			    		break;
			    	}
			    }
		    }
		    // check for finish
	    	System.out.println("Are u done?(n/y):");
	    	String r = reader.nextLine();
	    	if(r.equals("y")){
	    		break;
	    	}
	    }
	    
	  }  

}

class PrintTuple implements VoidFunction<Tuple2<Integer, Double[]>>{

	public void call(Tuple2<Integer, Double[]> t) throws Exception {
		System.out.print(t._1+" : ");
		for(int i=0; i<t._2.length; i++){
			System.out.print(t._2[i]+" ");
		}
		System.out.println();
	}
	
}

class PrintVecTuple implements VoidFunction<Tuple2<Integer, Vector3D>>{

	public void call(Tuple2<Integer, Vector3D> t) throws Exception {
		System.out.print(t._1+" : ");
		t._2().printValues();
		System.out.println();
	}
	
}

