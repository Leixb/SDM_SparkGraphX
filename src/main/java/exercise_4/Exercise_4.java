package exercise_4;

import com.clearspring.analytics.util.Lists;
import com.google.common.io.Resources;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.MetadataBuilder;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class Exercise_4 {

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		java.util.List<Row> vertices_list = new ArrayList<Row>();
		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);


		StructType vertices_schema = new StructType(new StructField[]{
			new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build()),
		});

		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

        try {
            Resources.readLines(Resources.getResource("wiki-vertices.txt"), Charset.defaultCharset()).forEach(line -> {
                String[] parts = line.split("\t", 2);
                vertices_list.add(RowFactory.create(parts[0], parts[1]));
            });
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

		// edges creation
		java.util.List<Row> edges_list = new ArrayList<Row>();

        try {
            Resources.readLines(Resources.getResource("wiki-edges.txt"), Charset.defaultCharset()).forEach(line -> {
                String[] parts = line.split("\t", 2);
                edges_list.add(RowFactory.create(parts[0], parts[1]));
            });
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list, 1000);

		StructType edges_schema = new StructType(new StructField[]{
			new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		GraphFrame gf = GraphFrame.apply(vertices,edges);

		System.out.println(gf);

        GraphFrame results = gf.pageRank().maxIter(10).resetProbability(0.15).run();

        results.vertices().select("id", "title", "pagerank").sort(col("pagerank").desc()).show(10);
        results.edges().select("src", "dst", "weight").sort(col("weight").desc()).show(10);

	}

}
