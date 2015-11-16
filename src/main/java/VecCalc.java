

import java.io.Serializable;

class VecCalc implements Serializable{
	
	public double x, y, z;
	
	public VecCalc(){
		x = 0; y = 0; z = 0;
	}
	
	public VecCalc(double a, double b, double c){
		x = a; y = b; z = c;
	}
	
	public VecCalc add(VecCalc mc){
		return new VecCalc(x+mc.x, y+mc.y, z+mc.z);
	}
	
	public void printValues(){
		System.out.print("x: "+x+"y: "+y+"z: "+z);
	}
	
	public String toString(){
		return (Double.toString(x)+" "+Double.toString(y)+" "+Double.toString(z)); 
	}
	
}

