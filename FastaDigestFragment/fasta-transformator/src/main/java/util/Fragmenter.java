package util;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.expasy.mzjava.core.ms.peaklist.PeakList;
import org.expasy.mzjava.core.ms.spectrum.IonType;
import org.expasy.mzjava.proteomics.mol.Peptide;
import org.expasy.mzjava.proteomics.ms.fragment.PeptideFragmenter;
import org.expasy.mzjava.proteomics.ms.spectrum.PeptideSpectrum;

import entities.FastaPeptide;
import entities.tSpectrum;

public class Fragmenter {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		List<Double> list = getSpectra(Peptide.parse("MEALLLSLYYPNDR"));
		for(Double mz : list){
			System.out.println(mz);
		}
	}
	
	
	public static tSpectrum getSpectra(FastaPeptide pep) {
		
		return null;
	}

	public static List<Double> getSpectra(Peptide pep) {

		
		PeptideFragmenter fragmenter = new PeptideFragmenter(EnumSet.of(IonType.b, IonType.y),
				PeakList.Precision.DOUBLE);
		PeptideSpectrum ps = fragmenter.fragment(pep, 2);
		List<Double> result = new ArrayList<Double>();
		
		for (int i = 0; i < ps.size(); i++) {
			result.add(ps.getMz(i));
		}
		
		result.add(pep.getMolecularMass());
		return result;

	}
	
public static List<tSpectrum> getTheoreticalSpectra(Peptide pep) {

		//X?
		if(pep.toString().toUpperCase().contains("X"))
			pep = Peptide.parse(pep.toString().replaceAll("X", ""));
		PeptideFragmenter fragmenter = new PeptideFragmenter(EnumSet.of(IonType.b, IonType.y),
				PeakList.Precision.DOUBLE);
		PeptideSpectrum ps = fragmenter.fragment(pep, 2);
		List<Double> result = new ArrayList<Double>();
		
		for (int i = 0; i < ps.size(); i++) {
			result.add(ps.getMz(i));
		}
		
		result.add(pep.getMolecularMass());
		List<tSpectrum> resultList = new ArrayList<>();
		tSpectrum spectrum = new tSpectrum();
		spectrum.setMzValues(result);
		spectrum.setCharge(2);
		spectrum.setPepmass(ps.getPrecursor().getMass());
		spectrum.setPepMz(ps.getPrecursor().getMz());
		
		resultList.add(spectrum);
		return resultList;

	}

}
