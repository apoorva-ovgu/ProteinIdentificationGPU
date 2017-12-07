package main;

import cassandra.CassandraConnector;
import cassandra.CassandraStorager;
import entities.FastaProtein;
import util.FastaTransformerService;

public class TransformerThread implements Runnable {

	private String protAsString;

	private CassandraConnector connector;
	

	public TransformerThread(String protAsString, CassandraConnector connector) {
		this.protAsString = protAsString;
		this.connector = connector;
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		runApp();
		
		System.out.println(System.currentTimeMillis() - start + " ms for Protein");

	}

	private void runApp() {
		FastaProtein prot = FastaTransformerService.GenerateProtein(this.protAsString);

		
		if (prot != null) {
			CassandraStorager.insertProtein(connector, prot);
		}

		//connector.close();
	}

}
