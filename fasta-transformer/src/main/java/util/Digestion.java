package util;

import java.util.List;

import org.expasy.mzjava.proteomics.mol.Peptide;
import org.expasy.mzjava.proteomics.mol.Protein;
import org.expasy.mzjava.proteomics.mol.digest.Protease;
import org.expasy.mzjava.proteomics.mol.digest.ProteinDigester;

import entities.FastaPeptide;
import entities.FastaProtein;



public class Digestion {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		/*
>sp|Q6GZX4|001R_FRG3G Putative transcription factor 001R OS=Frog virus 3 (isolate Goorha) GN=FV3-001R PE=4 SV=1
MAFSAEDVLKEYDRRRRMEALLLSLYYPNDRKLLDYKEWSPPRVQVECPKAPVEWNNP
PSEKGLIVGHFSGIKYKGEKAQASEVDVNKMCCWVSKFKDAMRRYQGIQTCKIPGKVL
SDLDAKIKAYNLTVEGVEGFVRYSRVTKQHVAAFLKELRHSKQYENVNLIHYILTDKR
VDIQHLEKDLVKDFKALVESAHRMRQGHMINVKYILYQLLKKHGHGPDGPDILTVKTG
SKGVLYDDSFRKIYTDLGWKFTPL
>sp|Q6GZX3|002L_FRG3G Uncharacterized protein 002L OS=Frog virus 3 (isolate Goorha) GN=FV3-002L PE=4 SV=1
MSIIGATRLQNDKSDTYSAGPCYAGGCSAFTPRGTCGKDWDLGEQTCASGFCTSQPLC
ARIKKTQVCGLRYSSKGKDPLVSAEWDSRGAPYVRCTYDADLIDTQAQVDQFVSMFGE
SPSLAERYCMRGVKNTAGELVSRVSSDADPAGGWCRKWYSAHRGPDQDAALGSFCIKN
PGAADCKCINRASDPVYQKVKTLHAYPDQCWYVPCAADVGELKMGTQRDTPTNCPTQV
CQIVFNMLDDGSVTMDDVKNTINCDFSKYVPPPPPPKPTPPTPPTPPTPPTPPTPPTP
PTPRPVHNRKVMFFVAGAVLVAILISTVRW
		 */
		
		List<Peptide> list = digestProtein("Q6GZX4", "MAFSAEDVLKEYDRRRRMEALL"
				+ "LSLYYPNDRKLLDYKEWSPPRVQVECPK"
				+ "APVEWNNPPSEKGLIVGHFSGIKYKGEK"
				+ "AQASEVDVNKMCCWVSKFKDAMRRYQGI"
				+ "QTCKIPGKVLSDLDAKIKAYNLTVEGVE"
				+ "GFVRYSRVTKQHVAAFLKELRHSKQYENV"
				+ "NLIHYILTDKRVDIQHLEKDLVKDFKALV"
				+ "ESAHRMRQGHMINVKYILYQLLKKHGHGPDG"
				+ "PDILTVKTGSKGVLYDDSFRKIYTDLGWKFTPL");
		
		for(Peptide pep : list){
			System.out.println(pep);
		}
		
	}

	
	
	public static List<Peptide> digestProtein (String accessionID, String proteinSequence){
		

		//default digester
		ProteinDigester digester = new ProteinDigester.Builder(Protease.TRYPSIN).build();

		//if not dafault parameter
//		ProteinDigester digester = new ProteinDigester.Builder(Protease.TRYPSIN)
//			    .missedCleavageMax(2).semi()
//			    .addFixedMod(AminoAcid.T, Modification.parseModification("H3PO4"))
//			    .build();
		Protein prot = new Protein(accessionID, proteinSequence);
		
		return digester.digest(prot);
	}
}
