package main;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.staticFileLocation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.Part;

import org.expasy.mzjava.proteomics.mol.Peptide;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import entities.FastaProtein;
import scala.collection.mutable.StringBuilder;
import util.Digestion;
import util.FastaTransformerService;
import util.Fragmenter;

public class ServerStart {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		File uploadDir = new File("upload");
		uploadDir.mkdir(); // create the upload directory if it doesn't exist
		System.out.println("upload folder created");
		staticFileLocation("upload");

		port(1234);
		get("/hello", (req, res) -> "Hello World");

		get("/", (req, res) -> "<form method='post' enctype='multipart/form-data'>"
				+ "    <input type='file' name='uploaded_file' accept='.fasta'>" + "    <button>Upload FASTA</button>"
				+ "</form>");

		post("/", (req, res) -> {

			Path tempFile = null;
			try {
				System.out.println(uploadDir.toPath().toString());
				tempFile = Files.createTempFile(uploadDir.toPath(), "", "");
				System.out.println(tempFile.toString());
				req.attribute("org.eclipse.jetty.multipartConfig", new MultipartConfigElement("/temp"));

				Part filePart = req.raw().getPart("uploaded_file");

				try (InputStream input = filePart.getInputStream()) {

					System.out.println("current line --> " + input);
					Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
				}

			} catch (IOException e) {
				e.printStackTrace();
			} catch (ServletException e1) {
				e1.printStackTrace();
			}
			//List<FastaProtein> resultList = new ArrayList<>();

			List<Integer> counterList = new ArrayList<>();
			
			counterList.add(0);
			Writer output;
			output = new BufferedWriter(new FileWriter(uploadDir.getPath() + File.separatorChar + tempFile.getFileName() + "_result.json"));
			Gson gson = new GsonBuilder().setPrettyPrinting().create();

			boolean isFirstTime = true;
			
			// read file into stream, try-with-resources
			try (Stream<String> stream = Files.lines(Paths.get(tempFile.toString()))) {

				StringBuilder result = new StringBuilder();
				
				
				
				stream.forEach((line) -> {					
					
					// System.out.println("current line --> " + line);

					// if line starts with > start
					if (line.trim().startsWith(">")) {
						counterList.set(0, counterList.get(0)+1);
						if(counterList.get(0)%1000==0){
							System.out.println(counterList.get(0));
						}
						if (!result.isEmpty()) {
							if (isFirstTime) {
								try {
									output.append("{");
								} catch (IOException e) {
									e.printStackTrace();
								}
							} else {
								try {
									output.append(",");
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
							FastaProtein prot = FastaTransformerService.GenerateProtein(result);

							if (prot != null) {
								// resultList.add(prot);
								try {
									output.append(gson.toJson(prot));
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
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
				FastaProtein prot = FastaTransformerService.GenerateProtein(result);
				if (prot != null) {
					// resultList.add(prot);
					try {
						output.append(gson.toJson(prot));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (IOException e) {

				e.printStackTrace();
			}

			//String json = gson.toJson(resultList);
			output.close();
			//System.out.println(json);

			return "successfull digested and fragmented";

			// return "<form method='post' enctype='multipart/form-data'>"
			// + " <input type='file' name='uploaded_file' accept='.fasta'>"
			// + " <button>Upload FASTA</button>" + "</form>";

		});

		post("/fasta", (request, response) -> {
			List<Peptide> list = Digestion.digestProtein("Q6GZX4", request.body().trim().replace(" ", ""));

			System.out.println(request.body());
			StringBuilder sb = new StringBuilder();
			sb.append("{[");
			for (Peptide pep : list) {
				sb.append("{" + pep + "},");
				System.out.println("\n" + pep);
				List<Double> listMZ = Fragmenter.getSpectra(Peptide.parse("MEALLLSLYYPNDR"));
				for (Double mz : listMZ) {
					System.out.println(mz);
					sb.append("{" + mz + "},");
				}
			}
			sb.append("]}");
			return sb.toString();
		});
	}

}
