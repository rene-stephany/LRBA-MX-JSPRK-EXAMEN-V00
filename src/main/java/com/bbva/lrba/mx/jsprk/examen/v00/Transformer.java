
package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> characteristics = datasetsFromRead.get("sourceAlias1");
        Dataset<Row> movies = datasetsFromRead.get("sourceAlias2");
        Dataset<Row> streaming = datasetsFromRead.get("sourceAlias3");

        Dataset<Row> cUnionM = characteristics.unionByName(movies, true);
        Dataset<Row> cUnionMUnionS = cUnionM.select(cUnionM.col("*"),
                functions.when(cUnionM.col("code").equalTo("NFLX"),"Netflix")
                        .when(cUnionM.col("code").equalTo("DSNY+"),"Disney+")
                        .when(cUnionM.col("code").equalTo("AMZN"),"Amazon Prime")
                        .alias("service"));
        cUnionMUnionS = cUnionMUnionS.select(cUnionMUnionS.col("*"),
                functions.lit(functions.current_timestamp()).alias("date"));

        datasetsToWrite.put("targetAlias1",cUnionMUnionS);

        Dataset<Row> countMovies = cUnionMUnionS.filter(functions.col("type").equalTo("Movie"))
                .groupBy("service").count().as("rank_movies");
        countMovies = countMovies.select(countMovies.col("*"),
                functions.lit(functions.current_timestamp()).alias("date"));

        datasetsToWrite.put("targetAlias2",countMovies);

        Dataset<Row> directoresDF = cUnionMUnionS.filter(functions.col("service").equalTo("Amazon Prime"))
                .filter(functions.col("type").equalTo("Movie"))
                .orderBy(functions.col("director"));

        datasetsToWrite.put("targetAlias3",directoresDF);

        Dataset<Row> seriesMoviesDuration = cUnionMUnionS.filter(functions.col("duration").equalTo("10 Seasons")
                .or(functions.col("duration").equalTo("182 min")));

        datasetsToWrite.put("targetAlias4",seriesMoviesDuration);


        return datasetsToWrite;
    }

}