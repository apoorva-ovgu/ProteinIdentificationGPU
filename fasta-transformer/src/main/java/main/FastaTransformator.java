package main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import cassandra.CassandraConnector;
import scala.collection.mutable.StringBuilder;

public class FastaTransformator {

	private static final String FASTA_PATH = System.getenv("FASTA_PATH") != null ? System.getenv("FASTA_PATH")
			: "./test.fasta";

	private static final String CASSANDRA_IP_ADRESS = System.getenv("CASSANDRA_IP_ADRESS") != null
			? System.getenv("CASSANDRA_IP_ADRESS") : "localhost";

	private static final String CASSANDRA_PORT_ADRESS = System.getenv("CASSANDRA_PORT_ADRESS") != null
			? System.getenv("CASSANDRA_PORT_ADRESS") : "9042";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		File fastaFile = new File(FASTA_PATH);

		CassandraConnector connector = new CassandraConnector();

		connector.connect(CASSANDRA_IP_ADRESS, Integer.valueOf(CASSANDRA_PORT_ADRESS));

		try (Stream<String> stream = Files.lines(Paths.get(fastaFile.getPath()))) {

			StringBuilder result = new StringBuilder();

			ExecutorService executor = Executors.newFixedThreadPool(2);

			stream.forEach((line) -> {
				if (line.trim().startsWith(">")) {
					if (!result.isEmpty()) {

						Runnable worker = new TransformerThread(result.toString(), connector);
						executor.execute(worker);

						result.clear();
						result.append(line + "\n");
					} else {

						result.append(line + "\n");

					}

				} else {
					if (!line.trim().isEmpty()) {
						result.append(line.trim());
					}
				}
			});
			Runnable worker = new TransformerThread(result.toString(), connector);
			executor.execute(worker);

			executor.shutdown();
			while (!executor.isTerminated()) {

				
				
			}
			connector.close();
			System.out.println("Finished all threads");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
