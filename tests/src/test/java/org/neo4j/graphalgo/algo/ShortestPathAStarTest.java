package org.neo4j.graphalgo.algo;

import static org.mockito.Mockito.mock;

import java.util.function.DoubleConsumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.ShortestPathProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class ShortestPathAStarTest {
	
	private static GraphDatabaseAPI db;
	
	@BeforeClass
    public static void setup() throws KernelException {
		/* Singapore to Chiba
		 * Path nA (0NM) -> nB (29NM) -> nC (723NM) -> nD (895NM) -> nE (996NM) -> nF (1353NM)
		 * 	    nG (1652NM) -> nH (2392NM) -> nX (2979NM)
		 * Distance = 2979 NM
		 * */
        String createGraph =
                "CREATE (nA:Node{name:'SINGAPORE', latitude:1.304444,longitude:103.717373})\n" + // start
                        "CREATE (nB:Node{name:'SINGAPORE STRAIT', latitude:1.1892, longitude:103.4689})\n" +
                        "CREATE (nC:Node{name:'WAYPOINT 68', latitude:8.83055556, longitude:111.8725})\n" +
                        "CREATE (nD:Node{name:'WAYPOINT 70', latitude:10.82916667, longitude:113.9722222})\n" +
                        "CREATE (nE:Node{name:'WAYPOINT 74', latitude:11.9675, longitude:115.2366667})\n" +
                        "CREATE (nF:Node{name:'SOUTH CHINA SEA', latitude:16.0728, longitude:119.6128})\n" +
                        "CREATE (nG:Node{name:'LUZON STRAIT', latitude:20.5325, longitude:121.845})\n" +
                        "CREATE (nH:Node{name:'WAYPOINT 87', latitude:29.32611111, longitude:131.2988889})\n" +
                        "CREATE (nX:Node{name:'CHIBA', latitude:35.562222, longitude:140.059187})\n" + // end
                        "CREATE\n" +
                        "  (nA)-[:TYPE {cost:29.0}]->(nB),\n" +
                        "  (nB)-[:TYPE {cost:694.0}]->(nC),\n" +
                        "  (nC)-[:TYPE {cost:172.0}]->(nD),\n" +
                        "  (nD)-[:TYPE {cost:101.0}]->(nE),\n" +
                        "  (nE)-[:TYPE {cost:357.0}]->(nF),\n" +
                        "  (nF)-[:TYPE {cost:299.0}]->(nG),\n" +
                        "  (nG)-[:TYPE {cost:740.0}]->(nH),\n" +
                        "  (nH)-[:TYPE {cost:587.0}]->(nX)";

        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }
        
        db.getDependencyResolver()
        .resolveDependency(Procedures.class)
        .registerProcedure(ShortestPathProc.class);
	}
	
	@AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }
	
	@Test
    public void testAStarData() throws Exception {
		final DoubleConsumer consumer = mock(DoubleConsumer.class);
        db.execute(
                "MATCH (start:Node{name:'SINGAPORE'}), (end:Node{name:'CHIBA'}) " +
                        "CALL algo.shortestPath.astar(start, end, 'cost') " +
                        "YIELD nodeId, cost RETURN nodeId, cost ")
                .accept(row -> {
                    long nodeId = row.getNumber("nodeId").longValue();
                    double distance = row.getNumber("cost").doubleValue();
                    consumer.accept(distance);
                    System.out.printf("%d:%.1f, ",
                            nodeId,
                            distance);
                    return true;
                });
    }
        
}
