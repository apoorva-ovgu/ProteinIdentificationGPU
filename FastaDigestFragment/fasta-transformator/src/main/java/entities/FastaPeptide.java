package entities;

import java.util.List;
import java.util.UUID;

public class FastaPeptide {

	private UUID uuid;
	private UUID protUuid;
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
	public UUID getUuid() {
		return uuid;
	}
	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}
	public UUID getProtUuid() {
		return protUuid;
	}
	public void setProtUuid(UUID protUuid) {
		this.protUuid = protUuid;
	}
	
}
