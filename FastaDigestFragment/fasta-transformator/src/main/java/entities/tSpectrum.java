package entities;

import java.util.List;
import java.util.UUID;

public class tSpectrum {

	private UUID uuid;
	private UUID pepUuid;
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
	public UUID getUuid() {
		return uuid;
	}
	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}
	public UUID getPepUuid() {
		return pepUuid;
	}
	public void setPepUuid(UUID pepUuid) {
		this.pepUuid = pepUuid;
	}
	
}
