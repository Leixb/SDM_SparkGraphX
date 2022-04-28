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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg
            extends AbstractFunction3<Long, Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>>
            implements Serializable {
        @Override
        public Tuple2<Long, Integer> apply(Long vertexID, Tuple2<Long, Integer> vertexValue,
                Tuple2<Long, Integer> message) {
            if (message._2() == Integer.MAX_VALUE) { // superstep 0
                return vertexValue;
            }

            // superstep > 0
            //
            // Since we already check the condition before sending the message
            // and on the merge, we can safely assume that the message is the
            // less than the current value.
            return message;
        }
    }

    private static class sendMsg extends
            AbstractFunction1<EdgeTriplet<Tuple2<Long, Integer>, Integer>, Iterator<Tuple2<Object, Tuple2<Long, Integer>>>>
            implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Long, Integer>>> apply(
                EdgeTriplet<Tuple2<Long, Integer>, Integer> triplet) {
            Integer edgeCost = triplet.attr();

            Tuple2<Object, Tuple2<Long, Integer>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Tuple2<Long, Integer>> dstVertex = triplet.toTuple()._2();

            Integer dstDist = dstVertex._2()._2();
            Integer currDist = sourceVertex._2()._2();

            // If the destination vertex has not been visited, or the distance to the destination vertex through
            // this edge is less than the current distance, then send a message
            // with the edge *source* vertex id and the distance.
            if (currDist != Integer.MAX_VALUE && dstDist > currDist + edgeCost) {
                return JavaConverters
                        .asScalaIteratorConverter(
                                Arrays.asList(new Tuple2<Object, Tuple2<Long, Integer>>(triplet.dstId(),
                                        new Tuple2<Long, Integer>(triplet.srcId(), currDist + edgeCost))).iterator())
                        .asScala();
            }

            // Otherwise, no message to send
            return JavaConverters
                    .asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Long, Integer>>>().iterator())
                    .asScala();
        }
    }

    private static class merge
            extends AbstractFunction2<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>>
            implements Serializable {
        @Override
        public Tuple2<Long, Integer> apply(Tuple2<Long, Integer> o, Tuple2<Long, Integer> o2) {
            // In case of a collision, prefer the smaller distance
            if (o._2() < o2._2()) {
                return o;
            } else {
                return o2;
            }
        }
    }

    private static Tuple2<Object, Tuple2<Long, Integer>> newVertex (Long id, Integer dist) {
        return new Tuple2<Object, Tuple2<Long, Integer>>(id, new Tuple2<Long, Integer>(Long.MAX_VALUE, dist));
    }

    // We save both the path cost and the vertex from where we came from in a
    // Tuple. This allows to compute the final path by tracing back the vertices
    // from which we came. Since the short's path is a tree, we know that there
    // are no cycles so we can trace-back without worrying about infinite loops.
    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        // For each vertex, we store the distance to the source vertex and the
        // vertex from which we came in a tuple Tuple<Long, Integer>.
        // The first value is the source vertex, the second is the distance.
        //
        // We need to adapt all the methods from exercise 2 to take a
        // Tuple2<Long, Integer> instead of just an Integer.
        //
        // Since the initialization is quite verbose, we use a function
        // `newVertex` to simplify it and avoid errors.
        List<Tuple2<Object, Tuple2<Long, Integer>>> vertices = Lists.newArrayList(
            newVertex(1l, 0), // Source vertex
            newVertex(2l, Integer.MAX_VALUE),
            newVertex(3l, Integer.MAX_VALUE),
            newVertex(4l, Integer.MAX_VALUE),
            newVertex(5l, Integer.MAX_VALUE),
            newVertex(6l, Integer.MAX_VALUE),
            newVertex(2l, Integer.MAX_VALUE));
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l, 2l, 4), // A --> B (4)
                new Edge<Integer>(1l, 3l, 2), // A --> C (2)
                new Edge<Integer>(2l, 3l, 5), // B --> C (5)
                new Edge<Integer>(2l, 4l, 10), // B --> D (10)
                new Edge<Integer>(3l, 5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, Tuple2<Long, Integer>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2<Long, Integer>, Integer> G = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(),
                new Tuple2<Long, Integer>(Long.MAX_VALUE, Integer.MAX_VALUE), StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        List<Tuple2<Object, Tuple2<Long, Integer>>> finalVertices = ops
                .pregel(new Tuple2<Long, Integer>(Long.MAX_VALUE, Integer.MAX_VALUE),
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD().collect();

        // Once the computation is done, we have to trace back the path from the
        // "linked list" of vertices. To do so, we use a HashMap to store the
        // information from the vertices so that we can process it quickly.
        //
        // Doing this, allows us to compute the path quickly for each vertex in linear
        // time. Instead of O(n^2) since we dont need to traverse the whole
        // vertex list to find each vertex in the path.

        Map<Long, Long> pathFrom = new HashMap<Long, Long>();

        finalVertices.forEach(v -> {
            pathFrom.put((Long) v._1(), v._2()._1());
        });

        // Now we run through the vertices and print the path with the helper
        // function `getPath`

        finalVertices.forEach(v -> {
            System.out.println("Minimum cost to get from " + labels.get(1l) + " to " + labels.get(v._1()) + " is " +
                    getPath(pathFrom, (Long) v._1(), labels).toString() + " with cost " + v._2()._2());
        });
    }

    // Helper function to trace back the path from the destincation vertex to
    // the source and reverse it.
    public static List<String> getPath(Map<Long, Long> pathFrom, Long position, Map<Long, String> labels) {
        List<String> path = new ArrayList<>();

        // As long as there is a previous vertex to visit, we add it to the
        // path and move along.
        while (position != Long.MAX_VALUE) {
            path.add(labels.get(position));
            position = pathFrom.get(position);
        }

        // Once finished, we reverse the path and return it.
        Collections.reverse(path);
        return path;
    }

}
