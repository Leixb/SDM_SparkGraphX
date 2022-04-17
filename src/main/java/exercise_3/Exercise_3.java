package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long,Tuple2<Long,Integer>,Tuple2<Long,Integer>,Tuple2<Long,Integer>> implements Serializable {
        @Override
        public Tuple2<Long,Integer> apply(Long vertexID, Tuple2<Long,Integer> vertexValue, Tuple2<Long,Integer> message) {
            if (message._2() == Integer.MAX_VALUE) { // superstep 0
                return vertexValue;
            }

            // superstep > 0
            return message;
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Long,Integer>, Integer>, Iterator<Tuple2<Object,Tuple2<Long,Integer>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Long,Integer>>> apply(EdgeTriplet<Tuple2<Long,Integer>, Integer> triplet) {
            Integer edgeCost = triplet.attr();

            Tuple2<Object,Tuple2<Long,Integer>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Tuple2<Long,Integer>> dstVertex = triplet.toTuple()._2();

            Integer dstDist = dstVertex._2()._2();
            Integer currDist = sourceVertex._2()._2();

            if (currDist != Integer.MAX_VALUE && (dstDist == Integer.MAX_VALUE || dstDist > currDist + edgeCost)) {
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Tuple2<Long,Integer>>(triplet.dstId(),
                    new Tuple2<Long,Integer>(triplet.srcId(), currDist + edgeCost))).iterator()).asScala();
            } else {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Tuple2<Long,Integer>>>().iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            return null;
        }
    }

	public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Tuple2<Object,Tuple2<Long,Integer>>> vertices = Lists.newArrayList(
                new Tuple2<Object,Tuple2<Long,Integer>>(1l, new Tuple2<Long,Integer>(Long.MAX_VALUE,0)),
                new Tuple2<Object,Tuple2<Long,Integer>>(2l, new Tuple2<Long,Integer>(Long.MAX_VALUE,Integer.MAX_VALUE)),
                new Tuple2<Object,Tuple2<Long,Integer>>(3l, new Tuple2<Long,Integer>(Long.MAX_VALUE,Integer.MAX_VALUE)),
                new Tuple2<Object,Tuple2<Long,Integer>>(4l, new Tuple2<Long,Integer>(Long.MAX_VALUE,Integer.MAX_VALUE)),
                new Tuple2<Object,Tuple2<Long,Integer>>(5l, new Tuple2<Long,Integer>(Long.MAX_VALUE,Integer.MAX_VALUE)),
                new Tuple2<Object,Tuple2<Long,Integer>>(6l, new Tuple2<Long,Integer>(Long.MAX_VALUE,Integer.MAX_VALUE))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object,Tuple2<Long,Integer>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2<Long,Integer>,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(), new Tuple2<Long,Integer>(Long.MAX_VALUE, Integer.MAX_VALUE), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new Tuple2<Long,Integer>(Long.MAX_VALUE, Integer.MAX_VALUE),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Tuple2.class))
            .vertices()
            .toJavaRDD()
            .foreach(v -> {
                Tuple2<Object,Tuple2<Long,Integer>> vertex = (Tuple2<Object,Tuple2<Long,Integer>>)v;
                System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1())+" is "+vertex._2()._2());
            });
	}

}
