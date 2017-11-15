package entities;

import java.util.List;

public class FastaPeptide {

	
	private String sequence;
	private List<tSpectrum> spectrumList;
	public String getSequence() {
		return sequence;
	}
	public void setSequence(String sequence) {
		this.sequence = sequence;
	}
	public List<tSpectrum> getSpectrumList() {
		return spectrumList;
	}
	public void setSpectrumList(List<tSpectrum> spectrumList) {
		this.spectrumList = spectrumList;
	}
	
}
