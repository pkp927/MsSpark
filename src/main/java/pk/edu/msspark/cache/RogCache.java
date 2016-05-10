package pk.edu.msspark.cache;

import java.io.File;
import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import pk.edu.msspark.resultRDD.ScalarRDD;
import pk.edu.msspark.utils.SelectParameters;
import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

public class RogCache {
	
	  public static SelectParameters checkROGcache(SQLContext sqlContext, String rogCacheLoc, SelectParameters param){
		  	//File f = new File(rogCacheLoc+"/rogCache.parquet");
		  	//if(!f.exists()){param.cached = false; return param;} 
		    DataFrame parquetFile = sqlContext.read().parquet(rogCacheLoc+"/rogCache.parquet");
		if(parquetFile==null){param.cached = false; return param;} 
	    	parquetFile.registerTempTable("parquetFile");
	    	DataFrame result;

	    	String query = "SELECT DISTINCT * FROM parquetFile "
					+ "WHERE frameNo >= "+param.firstFrame+" AND frameNo <= "+param.lastFrame;
	    	
	    	if(!param.skip.isEmpty()) query = query + " AND frameNo NOT IN ( "+param.skip.trim().replace(" ", " , ")+" )";
	    	if(param.minBound != null) query = query + " AND minx="+param.minBound.x+" AND miny="+param.minBound.y+ " AND minz="+param.minBound.z;
	    	else query = query + " AND minx=0 AND miny=0 AND minz=0";	    	 
	    	if(param.maxBound != null) query = query + " AND maxx="+param.maxBound.x+" AND maxy="+param.maxBound.y+ " AND maxz="+param.maxBound.z;
	    	else query = query + " AND maxx=0 AND maxy=0 AND maxz=0";
	    	if(param.axis != null) query = query + " AND axis="+param.axis;
		    else query = query + " AND axis=\"z\"";
	    	
	    	result = sqlContext.sql(query);
	    	result.show();
	    	long n1 = result.count();
	    	long n2 = param.lastFrame - param.firstFrame - param.skip.split("\\s+").length+1;
	    	if(n1<n2) param.cached = false;
	    	else param.cached = true;
	    	return param;
	  }

	  public static void cacheROGresult(SQLContext sqlContext, ScalarRDD ROG, String rogCacheLoc, SelectParameters param){
		    final Vector3D minB = param.minBound; 
	        final Vector3D maxB = param.maxBound; 
	        final String axis = param.axis;
	        
	        JavaRDD<ROGschema> ROGCache = ROG.getScalarRDD().map(
	        		new Function<Tuple2<Integer, Double>, ROGschema>(){

	    				public ROGschema call(Tuple2<Integer, Double> t) throws Exception {
	    					ROGschema ms = new ROGschema();
	    					ms.setFrameNo(t._1);
	    					
	    					if(minB != null){
	     						ms.setMinx(minB.x);
	     						ms.setMiny(minB.y);
	     						ms.setMinz(minB.z);
	    					}
	    					if(maxB != null){
	     						ms.setMaxx(maxB.x);
	     						ms.setMaxy(maxB.y);
	     						ms.setMaxz(maxB.z);
	    					}
	    					
	    					ms.setROG(t._2());
	    					ms.setAxis(axis);
	    					
	    					return ms;
	    				}
	        			
	        		});

	        DataFrame ROGdf = sqlContext.createDataFrame(ROGCache, ROGschema.class);
	        ROGdf.show();
	        ROGdf.save(rogCacheLoc+"/rogCache.parquet", SaveMode.Append);
	  }

	  public static class ROGschema implements Serializable{
		  private int frameNo;
		  private double minx;
		  private double miny;
		  private double minz;
		  private double maxx;
		  private double maxy;
		  private double maxz;
		  private double ROG;	
		  private String axis;
		
		public int getFrameNo() {
			return frameNo;
		}
		public void setFrameNo(int frameNo) {
			this.frameNo = frameNo;
		}
		public double getROG() {
			return ROG;
		}
		public void setROG(double ROG) {
			this.ROG = ROG;
		}
		public double getMaxz() {
			return maxz;
		}
		public void setMaxz(double maxz) {
			this.maxz = maxz;
		}
		public double getMaxy() {
			return maxy;
		}
		public void setMaxy(double maxy) {
			this.maxy = maxy;
		}
		public double getMaxx() {
			return maxx;
		}
		public void setMaxx(double maxx) {
			this.maxx = maxx;
		}
		public double getMinz() {
			return minz;
		}
		public void setMinz(double minz) {
			this.minz = minz;
		}
		public double getMiny() {
			return miny;
		}
		public void setMiny(double miny) {
			this.miny = miny;
		}
		public double getMinx() {
			return minx;
		}
		public void setMinx(double minx) {
			this.minx = minx;
		}
		public String getAxis() {
			return axis;
		}
		public void setAxis(String axis) {
			this.axis = axis;
		}
	  }

}
