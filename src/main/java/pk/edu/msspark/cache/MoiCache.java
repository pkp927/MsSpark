package pk.edu.msspark.cache;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import pk.edu.msspark.resultRDD.VectorRDD;
import pk.edu.msspark.utils.SelectParameters;
import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

public class MoiCache {
	
	  public static SelectParameters checkMOIcache(SQLContext sqlContext, String moiCacheLoc, SelectParameters param){
		    DataFrame parquetFile = sqlContext.read().parquet(moiCacheLoc+"/moiCache.parquet");
	    	parquetFile.registerTempTable("parquetFile");
	    	DataFrame result = sqlContext.sql("SELECT DISTINCT * FROM parquetFile "
	    			+ "WHERE frameNo >= "+param.firstFrame+" AND frameNo <= "+param.lastFrame
	    			+ " AND frameNo NOT IN ( "+param.skip.trim().replace(" ", " , ")+" )");
	    	result.show();
	    	long n1 = result.count();
	    	long n2 = param.lastFrame - param.firstFrame - param.skip.split("\\s+").length;
	    	if(n1<n2) param.cached = false;
	    	return param;
	  }

	  public static void cacheMOIresult(SQLContext sqlContext, VectorRDD MOI, String moiCacheLoc, SelectParameters param){
		    final Vector3D minB = param.minBound; 
	        final Vector3D maxB = param.maxBound; 
	        
	        JavaRDD<MOIschema> moiCache = MOI.getVectorRDD().map(
	        		new Function<Tuple2<Integer, Vector3D>, MOIschema>(){

	    				public MOIschema call(Tuple2<Integer, Vector3D> t) throws Exception {
	    					MOIschema ms = new MOIschema();
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
	    					
	    					ms.setMOIx(t._2().x);
	    					ms.setMOIy(t._2().y);
	    					ms.setMOIz(t._2().z);
	    					return ms;
	    				}
	        			
	        		});

	        DataFrame MOIdf = sqlContext.createDataFrame(moiCache, MOIschema.class);
	        MOIdf.show();
	        MOIdf.save(moiCacheLoc+"/moiCache.parquet", SaveMode.Append);
	  }

	  public static class MOIschema implements Serializable{
		  private int frameNo;
		  private double minx;
		  private double miny;
		  private double minz;
		  private double maxx;
		  private double maxy;
		  private double maxz;
		  private double MOIx;
		  private double MOIy;
		  private double MOIz;		  
		
		public int getFrameNo() {
			return frameNo;
		}
		public void setFrameNo(int frameNo) {
			this.frameNo = frameNo;
		}
		public double getMOIz() {
			return MOIz;
		}
		public void setMOIz(double mOIz) {
			MOIz = mOIz;
		}
		public double getMOIy() {
			return MOIy;
		}
		public void setMOIy(double mOIy) {
			MOIy = mOIy;
		}
		public double getMOIx() {
			return MOIx;
		}
		public void setMOIx(double mOIx) {
			MOIx = mOIx;
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
	  }

}
