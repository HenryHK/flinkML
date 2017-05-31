package ml;

import java.util.Collections;
import java.util.LinkedList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class MLGenreTopMoviesNaive {
	public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String movieDir = params.getRequired("movie-dir");
        if(movieDir.charAt(movieDir.length() - 1) != '/') {
            movieDir = movieDir + '/';
        }

        // Read in the ratings file.
        DataSet<String> ratingsRaw = env.readTextFile(movieDir + "ratings.csv");

        // Count how many ratings each movie has received, outputting:
        // (movieId, ratingCount)
        DataSet<Tuple2<String, Integer>> movieRatingCount =
            ratingsRaw
                .map(line -> new Tuple2<String,Integer>(line.split(",")[1], 1))
                .groupBy(0)
                .sum(1);

        // Read in the movies CSV file, whose initial format is:
        // (movieId, name, genreList)
        //
        // We'll flatMap the genre list:
        // (movieId, name, genre)
        DataSet<Tuple3<String, String, String>> movieNameGenre =
            env.readTextFile(movieDir + "movies.csv")
                .flatMap((line, out) -> {
                    String[] values = line.split(",");
                    int length = values.length;

                    if(length >= 3) {
                        String title = values[1];

                        if(length > 3) { // Movie title contains commas?
                            for(int i = 2; i < length - 1; i++) {
                                title = title + "," + values[i];
                            }
                        }

                        String movieId = values[0];
                        String[] genres = values[values.length - 1].split("\\|");

                        for(String genre : genres) {
                            out.collect(new Tuple3<String, String, String>(movieId, title, genre));
                        }
                    }
                });

        // Join the two datasets to find the rating of each movie:
        // (movieId, ratingCount), (movieId, name, genre)
        //
        // ... to:
        // (genre, name, ratingCount)
        DataSet<Tuple3<String, String, Integer>> genreMovieRatingCount =
            movieNameGenre
                .join(movieRatingCount)
                .where(0)
                .equalTo(0)
                .projectFirst(2, 1)
                .projectSecond(1);

        // Map the dataset to:
        // (genre, MovieRatingCount)
        DataSet<Tuple2<String, MovieRatingCount>> genreMRC =
            genreMovieRatingCount
                .map(tuple -> new Tuple2<>(
                    tuple.f0,
                    new MovieRatingCount(tuple.f1, tuple.f2)
                ));

        // Group movies for each genre and sort based on rating count, going from:
        // (genre, MovieRatingCount)
        //
        // ... to:
        // (genre, [MovieRatingCount1, ..., MovieRatingCount5])
        DataSet<Tuple2<String, LinkedList<MovieRatingCount>>> genreTop5MoviesByCount =
            genreMRC
                .groupBy(0)
                .reduceGroup((tuples, out) -> {
                    String genre = "";
                    LinkedList<MovieRatingCount> vList = new LinkedList<>();

                    for(Tuple2<String, MovieRatingCount> tuple : tuples) {
                        genre = tuple.f0;
                        vList.add(tuple.f1);
                    }

                    Collections.sort(vList);
                    
                    LinkedList<MovieRatingCount> results = new LinkedList<>();
                    for (int i = 0; i <5; i ++){
                        results.add(vList.get(i));
                    }

                    out.collect(new Tuple2<>(genre, results));
                });

        // End the program by writing the output!
        if(params.has("output")) {
            genreTop5MoviesByCount.writeAsCsv(params.get("output"));

            env.execute();
        } else {
            // Always limit direct printing
            // as it requires pooling all resources into the driver.
            System.err.println("No output location specified; printing first 100.");
            genreTop5MoviesByCount.first(100).print();
        }
	}
}
