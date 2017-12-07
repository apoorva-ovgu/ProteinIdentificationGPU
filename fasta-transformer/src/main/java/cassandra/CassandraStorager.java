package cassandra;

import java.util.UUID;

import com.datastax.driver.core.SimpleStatement;

import entities.FastaPeptide;
import entities.FastaProtein;
import entities.tSpectrum;

public class CassandraStorager {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static void insertProtein(CassandraConnector connector, FastaProtein prot) {

		
		
		SimpleStatement simpleStatement = new SimpleStatement(
				"INSERT INTO fasta.prot_table (id,protein_description,protein_sequence,protein_accession) VALUES(?, ?, ?, ?)",
				prot.getUuid(), prot.getDescription(), prot.getSequence(), prot.getAccession());

		//connector.getSession().execute(simpleStatement);
		StringBuilder values = new StringBuilder();

		for (FastaPeptide pep : prot.getPeptideList()) {

			SimpleStatement simpleStatementPep = new SimpleStatement(
					"INSERT INTO fasta.prot_pep (id,protein_id,peptide_id) VALUES(?, ?, ?)", UUID.randomUUID(),
					prot.getUuid(), pep.getUuid());

			//connector.getSession().execute(simpleStatementPep);

			for (tSpectrum spec : pep.getSpectrumList()) {

				values = new StringBuilder();
				for (Double val : spec.getMzValues()) {
					values.append(val + ",");
				}
				values = values.deleteCharAt(values.length() - 1);
				SimpleStatement simpleStatementSpec = new SimpleStatement(
						"INSERT INTO fasta.pep_spec (peptide_id,spectrum_id,peptide_sequence,spectrum_charge,pep_mass,pep_mz,mz_values, pepmass) VALUES(?, ?, ?, ?,?,?,?)",
						pep.getUuid(), spec.getUuid(), pep.getSequence(), spec.getCharge(), spec.getPepmass(),
						spec.getPepMz(), values.toString());

				connector.getSession().execute(simpleStatementSpec);

			}
		}
	}

}
