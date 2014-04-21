package org.yusuf;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.logging.BufferingLogger;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;


public class Application {

    private static enum RelTypes implements RelationshipType {
        KNOWS;
    }

    static Node yusuf;

    public static void main(String[] args) {

        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder("./data")
                .setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
                .setConfig(GraphDatabaseSettings.relationship_keys_indexable, "true")
                .newGraphDatabase();

        registerShutdownHook(graphDb);

        try (Transaction tx = graphDb.beginTx()) {
            Schema schema = graphDb.schema();
            schema.indexFor(DynamicLabel.label("User"))
                    .on("username")
                    .create();


            tx.success();
        }

        Label label = DynamicLabel.label("User");

        try (Transaction tx = graphDb.beginTx()) {

            yusuf = graphDb.createNode(label);
            yusuf.setProperty("username", "yusuf");

            tx.success();
        }

        try (Transaction tx = graphDb.beginTx()) {

            for (int i = 0; i < 1000; i++) {
                Node f = graphDb.createNode(label);
                f.setProperty("username", UUID.randomUUID().toString().substring(0, 5));

                if (i == 50) {
                    f.createRelationshipTo(yusuf, RelTypes.KNOWS);
                }

                for (int j = 0; j < 50; j++) {
                    Node fof =
                            graphDb.createNode(label);
                    fof.setProperty("username", UUID.randomUUID().toString().substring(0, 5));
                    f.createRelationshipTo(fof, RelTypes.KNOWS);
                }
            }

            tx.success();
        }


        try (Transaction tx = graphDb.beginTx()) {
            ExecutionEngine engine = new ExecutionEngine(graphDb, new BufferingLogger());

            for (int i = 0; i < 50; i++) {
                StringBuilder sb = new StringBuilder();
                long start = System.currentTimeMillis();
                ExecutionResult result = engine.execute("MATCH (me { username: 'yusuf' })-[:KNOWS*2..2]-(friend_of_friend) " +
                        "WHERE NOT (me)-[:KNOWS]-(friend_of_friend) " +
                        "RETURN ID(friend_of_friend)");

//                System.out.println(result.dumpToString());
//                System.out.println(result.queryStatistics().toString());
                for (Map.Entry<String, Object> column : result.javaIterator().next().entrySet()) {
                    sb.append(column.getKey()).append(": ").append(column.getValue()).append("\n");
                }

                long total = System.currentTimeMillis() - start;

                System.out.println(total);
                System.out.println(sb.toString());
            }

            tx.success();
        }

        benchmarkNodeFind(graphDb);

    }

    private static void benchmarkNodeFind(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {

            Label label = DynamicLabel.label("User");

            long start = System.currentTimeMillis();
            ResourceIterator<Node> users =
                    graphDb.findNodesByLabelAndProperty(label, "username", "yusuf").iterator();
            long total = System.currentTimeMillis() - start;
            System.out.println(total);

            ArrayList<Node> userNodes = new ArrayList<>();

            while (users.hasNext()) {
                userNodes.add(users.next());
            }

            for (Node node : userNodes) {
                System.out.println("The username of user is " + node.getProperty("username"));
            }

            tx.success();
        }
    }


    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }
}
