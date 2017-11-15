package entities;

import java.util.List;

public class tSpectrum {

	private int charge;
	private double pepMass;
	private double pepMz;
	private List<Double> mzValues;
	public int getCharge() {
		return charge;
	}
	public void setCharge(int charge) {
		this.charge = charge;
	}
	public double getPepmass() {
		return pepMass;
	}
	public void setPepmass(double pepmass) {
		this.pepMass = pepmass;
	}
	public List<Double> getMzValues() {
		return mzValues;
	}
	public void setMzValues(List<Double> mzValues) {
		this.mzValues = mzValues;
	}
	public double getPepMz() {
		return pepMz;
	}
	public void setPepMz(double pepMz) {
		this.pepMz = pepMz;
	}
	
}
