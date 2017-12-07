package entities;

import java.util.Set;
import java.util.UUID;

public class FastaProtein {

	
	private UUID uuid;
	private String accession;
	private String description;
	private String sequence;
	private Set<FastaPeptide> peptideList;
	
	
	
	public UUID getUuid() {
		return uuid;
	}




	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}




	public String getAccession() {
		return accession;
	}




	public void setAccession(String accession) {
		this.accession = accession;
	}




	public String getDescription() {
		return description;
	}




	public void setDescription(String description) {
		this.description = description;
	}




	public String getSequence() {
		return sequence;
	}




	public void setSequence(String sequence) {
		this.sequence = sequence;
	}




	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}




	public Set<FastaPeptide> getPeptideList() {
		return peptideList;
	}




	public void setPeptideList(Set<FastaPeptide> peptideList) {
		this.peptideList = peptideList;
	}

}
