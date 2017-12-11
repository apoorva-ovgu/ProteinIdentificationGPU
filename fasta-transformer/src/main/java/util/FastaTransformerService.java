package util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.expasy.mzjava.proteomics.mol.Peptide;
import org.expasy.mzjava.proteomics.mol.Protein;
import org.expasy.mzjava.proteomics.mol.digest.Protease;
import org.expasy.mzjava.proteomics.mol.digest.ProteinDigester;

import entities.FastaPeptide;
import entities.FastaProtein;
import java.util.stream.Collectors;

public class FastaTransformerService {

	public static Set<FastaPeptide> digestAndFragementProtein(FastaProtein fastaProtein) {

		ProteinDigester digester = new ProteinDigester.Builder(Protease.TRYPSIN).build();

		Protein prot = new Protein(fastaProtein.getAccession(), fastaProtein.getSequence());

		List<Peptide> list = digester.digest(prot);
		Set<FastaPeptide> result = new HashSet<>();
		
		list = list.stream().filter(it->(it.toString().length()>=5&&it.toString().length()<=50)).collect(Collectors.toList());
		for (Peptide pep : list) {

			FastaPeptide fPep = new FastaPeptide();
			fPep.setUuid(UUID.randomUUID());
			fPep.setProtUuid(fastaProtein.getUuid());
			fPep.setSequence(pep.toString());
			fPep.setSpectrumList(Fragmenter.getTheoreticalSpectra(pep, fPep));

			result.add(fPep);

		}
		return result;
	}

	public static FastaProtein GenerateProtein(String proteinText) {
		//System.out.println("current protein --> " + proteinText.toString());
		// transform to Protein Object
		FastaProtein prot = new FastaProtein();
		// accession --> first line take the element after
		// the first pipe "|"
		prot.setAccession(proteinText.split("\n")[0].split("|")[1].trim());
		// description --> first line
		prot.setDescription(proteinText.split("\n")[0].trim());
		// sequence --> second line
		prot.setSequence(proteinText.split("\n")[1].trim());

		// unknown amino acid
		if (prot.getSequence().toUpperCase().indexOf("X") >= 0)
			return null;
		// unknown amino acid
		if (prot.getSequence().toUpperCase().indexOf("Z") >= 0)
			return null;
		// unknown amino acid
		if (prot.getSequence().toUpperCase().indexOf("B") >= 0)
			return null;

		// generate uuid
		prot.setUuid(UUID.randomUUID());
		// send the result
		prot.setPeptideList(FastaTransformerService.digestAndFragementProtein(prot));

		return prot;
	}

}
