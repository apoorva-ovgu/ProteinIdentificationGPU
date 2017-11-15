package main;

import java.util.List;

import org.expasy.mzjava.proteomics.mol.Peptide;

import util.Digestion;
import util.Fragmenter;

public class FastaTransformator {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		List<Peptide> list = Digestion.digestProtein("Q6GZX4", "MDFLNSSDQNLTSEELLNRMPSKILVSLTLSGLALMTTTINSLVIAAIIVTRKLHHPANYLICSLAVTDFLVAVLVMPFSIVYIVRESWIMGQVVCDIWLSVDITCCTCSILHLSAIALDRYRAITDAVEYARKRTPKHAGIMITIVWIISVFISMPPLFWRHQGTSRDDECIIKHBHIVSTIYSTFGAFYIPLALILILYYKIYRAAKTLYHKRQASRIAKEEVNGQVLLESGEKSTKSVSTSYVLEKSLSDPSTDFDKIHSTVRSLRSEFKHEKSWRRQKISGTRERKAATTLGLILGAFVICWLPFFVKELVVNVCDKCKISEEMSNFLAWLGYLNSLINPLIYTIFNEDFKKAFQKLVRCR");
		
		
		
		for(Peptide pep : list){
			System.out.println("\n" + pep);
			List<Double> listMZ = Fragmenter.getSpectra(pep);
			for(Double mz : listMZ){
				System.out.println(mz);
			}	
		}
	}

}
