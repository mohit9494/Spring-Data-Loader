package com.springboot.bookfinderdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.springboot.bookfinderdataloader.connection.DataStaxAstraProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BookfinderDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorLocation;

	@Value("${datadump.location.work}")
	private String workLocation;

	public static void main(String[] args) {
		SpringApplication.run(BookfinderDataLoaderApplication.class, args);
	}

	private void initAuthors() {

		Path path = Paths.get(authorLocation);

		try (Stream<String> lines = Files.lines(path)) {

			lines.forEach(line -> {

				String jsonString = line.substring(line.indexOf("{"));

				try {
					JSONObject jo = new JSONObject(jsonString);

					// Construct Author Object
					Author author = new Author();
					author.setId(jo.optString("key").replace("/authors/", ""));
					author.setName(jo.optString("name"));

					System.out.println(author.getName() + " ....");

					// Persist in Cassandra DB
					authorRepository.save(author);

				} catch (JSONException e) {
					e.printStackTrace();
				}

			});

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void initWorks() {

		Path path = Paths.get(workLocation);

		// 2009-12-11T01:57:19.964652

		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

		try (Stream<String> lines = Files.lines(path)) {

			lines.forEach(line -> {

				String json = line.substring(line.indexOf("{"));

				try {

					JSONObject jo = new JSONObject(json);

					// create book object
					Book b = new Book();

					b.setId(jo.getString("key").replace("/works/", ""));

					b.setName(jo.optString("title"));

					JSONObject descriptionObj = jo.optJSONObject("description");
					if (descriptionObj != null) {
						b.setDescription(descriptionObj.optString("value"));
					} else {
						b.setDescription("NA");
					}

					JSONObject pubObj = jo.optJSONObject("created");
					if (pubObj != null) {
						String dateString = pubObj.optString("value");
						b.setPublishedDate(LocalDate.parse(dateString, dateFormat));
					}

					JSONArray coverArray = jo.optJSONArray("covers");
					if (coverArray != null) {

						List<String> coverIds = new ArrayList<>();

						for (int i = 0; i < coverArray.length(); i++) {
							coverIds.add(coverArray.getString(i));
						}
						b.setCoverIds(coverIds);
					}

					// Adding author ids and names
					JSONArray authorsArr = jo.optJSONArray("authors");
					if (authorsArr != null) {
						List<String> authorIds = new ArrayList<>();

						for (int i = 0; i < authorsArr.length(); i++) {

							String authorId = authorsArr.getJSONObject(i).getJSONObject("author").getString("key")
									.replace("/authors/", "");

							authorIds.add(authorId);

						}

						b.setAuthorIds(authorIds);

						List<String> authorNames = b.getAuthorIds().stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (optionalAuthor.isPresent())
										return optionalAuthor.get().getName();
									else
										return "NA";
								})
								.collect(Collectors.toList());

						b.setAuthorNames(authorNames);
					}

					// Persist the book Object
					System.out.println("Saving Book Name... " + b.getName());
					bookRepository.save(b);

				} catch (JSONException e) {
					e.printStackTrace();
				}

			});

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {

		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

	@PostConstruct
	public void start() {

		System.out.println("******* Application Started *********");

		// initAuthors();
		initWorks();

	}

}
