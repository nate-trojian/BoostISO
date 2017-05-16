import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.FilteringIterator;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Author: Nate
 * Created on: 4/4/17
 * Description: Implementation of BoostIso for Project
 */
public class BoostIso {
    public static void main(String[] args) {
        if(args.length == 1) {
            BoostIso boostIso = new BoostIso(args[0]);
            Scanner in = new Scanner(System.in);
            String line;
            String[] parts;

            input:
            while(in.hasNextLine()) {
                line = in.nextLine();
                parts = line.split(" ");
                switch(parts[0]) {
                    case "build":
                        boostIso.buildAdaptedGraph(Label.label(parts[1]));
                        break;
                    case "search":
                        boostIso.naiveMatch(parts[1], parts[2]);
                        boostIso.naiveMatchSH(parts[1], parts[2]);
                        boostIso.naiveMatchBoost(parts[1], parts[2]);
                        boostIso.graphQLMatch(parts[1], parts[2]);
                        boostIso.graphQLMatchSH(parts[1], parts[2]);
                        boostIso.graphQLMatchBoost(parts[1], parts[2]);
                        break;
                    case "exit":
                    default:
                        break input;
                }
            }
            boostIso.close();
        } else usage();
    }

    private static void usage() {
        throw new IllegalArgumentException("usage: java BoostIso neo4jUrl");
    }

    private GraphDatabaseService db;

    public BoostIso(String neoPath) {
        File neoDir = new File(neoPath);
        System.out.println("----Opening Neo4j Connection----");
        db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(neoDir)
                .setConfig(GraphDatabaseSettings.pagecache_memory, "512M")
                .setConfig(GraphDatabaseSettings.string_block_size, "60")
                .setConfig(GraphDatabaseSettings.array_block_size, "300").newGraphDatabase();

    }

    /* Assumes Adapted Graph has not been built already
     * Steps to remove this assumption:
     * 1. Delete all hyper_SC edges at start of transaction
     *
     * Assumes nodes only have 1 internal label
     * Steps to remove this assumption:
     * 1. Make curLabel a set of labels (rename to curLabels)
     * 2. Set equivalence between other.getLabels and curLabels instead of if(!other.hasLabel(curLabel))
     *
     * Potential optimizations
     * 1. Move sEquivalence method to in function so we can reuse v.getRelationships used in adj
     *  - Reduces database calls by M+(M*M') where M is |adj(v)| and M' is avg(|adj(v')|) for all v' in adj(v)
     */
    public void buildAdaptedGraph(Label curGraph) {
        System.out.println("--Building Adapted Graph--");
        try(Transaction tx = db.beginTx()) {
            //Since stream is lazy and findNodes returns NodeProxy,
            //we can set vertices to hidden and they will be filtered when the stream reaches them
            db.findNodes(curGraph).stream()
                    .filter(v -> v.getProperty("hyper_isHidden", false).equals(false))
                    .forEach(v -> {
                        Label curLabel = null;
                        for(Label label : v.getLabels()) {
                            if(label.equals(curGraph)) continue;
                            curLabel = label;
                            break;
                        }
                        v.setProperty("hyper_isClique", false);
                        ArrayList<Node> SC = new ArrayList<>();
                        Iterator<Relationship> adj = v.getRelationships().iterator();
                        Relationship curEdge;
                        Node other;
                        while(adj.hasNext()) {
                            curEdge = adj.next();
                            other = curEdge.getOtherNode(v);
                            if(!other.hasLabel(curLabel)) continue;
                            if(sEquivalence(v, other)) {
                                v.setProperty("hyper_isClique", true);
                                other.setProperty("hyper_isHidden", true);
                                long[] sec = (long[])v.getProperty("hyper_SEC", new long[0]);
                                long[] newSec = Arrays.copyOf(sec, sec.length+1);
                                newSec[sec.length] = other.getId();
                                v.setProperty("hyper_SEC", newSec);
                            } else if(sContainment(v, other)) SC.add(other);  //SC-Children
                        }
                        if(v.getProperty("hyper_isClique").equals(false)) {
                            ResourceIterator<Node> twoStep = db.execute("MATCH (u:`" + curLabel + "`)--()--(v:`" + curLabel + "`) " +
                                    "WHERE id(u)=" + v.getId() + " AND NOT((u)--(v) OR id(u) = id(v)) " +
                                    "RETURN DISTINCT id(v)")
                                    .<Node>map(n -> db.getNodeById((Long) n.get("id(v)")));
                            while(twoStep.hasNext()) {
                                other = twoStep.next();
                                if(sEquivalence(v, other)) {
                                    other.setProperty("hyper_isHidden", true);
                                    long[] sec = (long[])v.getProperty("hyper_SEC", new long[0]);
                                    long[] newSec = Arrays.copyOf(sec, sec.length+1);
                                    newSec[sec.length] = other.getId();
                                    v.setProperty("hyper_SEC", newSec);
                                } else if(sContainment(v, other)) SC.add(other);  //SC-Children
                            }
                        }

                        //In place transitive reduction
                        //Sort descending on degree, work down the path
                        SC.sort((o1, o2) -> Integer.compare(o1.getDegree(), o2.getDegree()));
                        ArrayList<Node> paths = new ArrayList<>();
                        //The first in this list will always be a path, don't think it changes time so leaving it
                        for(Node n: SC) {
                            boolean flag = false;
                            for(Node p: paths) {
                                //Check for duplicates using cypher?
                                //neo4j offers bad edge check
                                //Returns with 1 or none, so hasNext works as a check
                                //Yes it's ugly
                                //No I don't care
                                if(db.execute("MATCH (u)-[:hyper_SC]-(v) " +
                                        "WHERE id(u)=" + p.getId() + " AND id(v)=" + n.getId() + " RETURN {result:true}").hasNext())
                                    continue;

                                if(sContainment(p, n)) {
                                    flag = true;
                                    p.createRelationshipTo(n, RelationshipType.withName("hyper_SC"));
                                    //Replace p with n as bottom of path
                                    paths.remove(p);
                                    paths.add(n);
                                    break;
                                }
                            }
                            if(!flag) {
                                paths.add(n);
                                v.createRelationshipTo(n, RelationshipType.withName("hyper_SC"));
                            }
                        }
                    });
            tx.success();
        }
        System.out.println("--Adapted Graph Built--");
    }

    // adj(u) - {v} > adj(v) - {u}
    private boolean sContainment(Node u, Node v) {
        Set<Long> adjU = StreamSupport.stream(u.getRelationships().spliterator(), false)
                .map(r -> r.getOtherNode(u).getId()).collect(Collectors.toSet());
        Set<Long> adjV = StreamSupport.stream(v.getRelationships().spliterator(), false)
                .map(r -> r.getOtherNode(v).getId()).collect(Collectors.toSet());
        if(adjV.size() > adjU.size()) return false;
        //Faster to remove now than filter after map (worse case equal but best case is faster)
        adjU.remove(v.getId());
        adjV.remove(u.getId());
        return adjU.containsAll(adjV);
    }

    private boolean sEquivalence(Node u, Node v) {
        Set<Long> adjU = StreamSupport.stream(u.getRelationships().spliterator(), false)
                .map(r -> r.getOtherNode(u).getId()).collect(Collectors.toSet());
        Set<Long> adjV = StreamSupport.stream(v.getRelationships().spliterator(), false)
                .map(r -> r.getOtherNode(v).getId()).collect(Collectors.toSet());
        System.out.println("U: " + adjU);
        System.out.println("V: " + adjV);
        if(adjU.size() > adjV.size()) return false;
        //Faster to remove now than filter after map (worse case equal but best case is faster)
        adjU.remove(v.getId());
        adjV.remove(u.getId());
        return adjU.containsAll(adjV) && adjV.containsAll(adjU);
    }

    public void naiveMatch(String queryGraphLbl, String targetGraphLbl) {
        try(Transaction tx = db.beginTx()) {
            long t = -System.currentTimeMillis();
            //Use Neo4j to get search space for each node
            HashMap<Node, ArrayList<Node>> searchSpace = new HashMap<>();

            ResourceIterator<Node> queryGraph = db.findNodes(Label.label(queryGraphLbl));
            Node n;
            ArrayList<Node> search;
            while(queryGraph.hasNext()) {
                n = queryGraph.next();
                search = new ArrayList<>();
                for(Label l: n.getLabels()) {
                    if(l.name().equals(queryGraphLbl)) continue; //At least two labels, we know one, but we need the others
                    //More nodes with secondary label than graph label, so get nodes based on that
                    ResourceIterator<Node> nodes = db.findNodes(Label.label(targetGraphLbl));
                    Iterator<Node> fromNode = new FilteringIterator<>(nodes, node -> node.hasLabel(l));

                    while(fromNode.hasNext()) {
                        search.add(fromNode.next());
                    }
                }
                //Simple optimization
                if(search.size() == 0) {
                    System.out.println("No Solutions");
                    return;
                }
                searchSpace.put(n, search);
            }

            //Pick order
            ArrayList<Node> order = new ArrayList<>(), size = new ArrayList<>();

            Iterator<Node> searchIt = searchSpace.keySet().iterator(); //Randomly sorts keys
            HashMap<Node, Integer> sizeMap = new HashMap<>();
            Node temp;
            while(searchIt.hasNext()) {
                temp = searchIt.next();
                sizeMap.put(temp, searchSpace.get(temp).size());
                size.add(temp);
            }
            size.sort((o1, o2) -> sizeMap.get(o1).compareTo(sizeMap.get(o2)));
            PriorityQueue<Node> queue = new PriorityQueue<>((o1, o2) -> sizeMap.get(o1).compareTo(sizeMap.get(o2)));

            while(!size.isEmpty()) {
                //Get the node with the lowest search space to start each connected area of the query
                queue.add(size.remove(0));

                //The queue can become empty without us getting a full ordering, so we have it in a bigger while loop
                Iterable<Relationship> edges;
                while(!queue.isEmpty()) {
                    temp = queue.remove();
                    order.add(temp);
                    size.remove(temp);
                    edges = temp.getRelationships();
                    for(Relationship r : edges) {
                        Node other = r.getOtherNode(temp);
                        //Don't add visited nodes (Would be quicker to have a boolean hashmap, but meh)
                        if(!order.contains(other) && !queue.contains(other)) {
                            queue.add(other);
                        }
                    }
                }
            }

            //Backtrack
            SolutionSet mappings = new SolutionSet(1000);
            backtrack(0, order, searchSpace, mappings);

            System.out.println("Time Took Naive: " + (t + System.currentTimeMillis()));
            mappings.printSolutions();
            tx.success();
        }
    }

    public void naiveMatchSH(String queryGraphLbl, String targetGraphLbl) {
        try(Transaction tx = db.beginTx()) {
            long t = -System.currentTimeMillis();
            //Use Neo4j to get search space for each node
            HashMap<Node, ArrayList<Node>> searchSpace = new HashMap<>();

            ResourceIterator<Node> queryGraph = db.findNodes(Label.label(queryGraphLbl));
            Node n;
            ArrayList<Node> search;
            while(queryGraph.hasNext()) {
                n = queryGraph.next();
                search = new ArrayList<>();
                for(Label l: n.getLabels()) {
                    if(l.name().equals(queryGraphLbl)) continue; //At least two labels, we know one, but we need the others
                    //More nodes with secondary label than graph label, so get nodes based on that
                    ResourceIterator<Node> nodes = db.findNodes(Label.label(targetGraphLbl));
                    Iterator<Node> fromNode = new FilteringIterator<>(nodes, node -> node.hasLabel(l));

                    while(fromNode.hasNext()) {
                        search.add(fromNode.next());
                    }
                }
                //Simple optimization
                if(search.size() == 0) {
                    System.out.println("No Solutions");
                    return;
                }
                searchSpace.put(n, search);
            }

            //Pick order
            ArrayList<Node> order = new ArrayList<>(), size = new ArrayList<>();

            Iterator<Node> searchIt = searchSpace.keySet().iterator(); //Randomly sorts keys
            HashMap<Node, Integer> sizeMap = new HashMap<>();
            Node temp;
            while(searchIt.hasNext()) {
                temp = searchIt.next();
                sizeMap.put(temp, searchSpace.get(temp).size());
                size.add(temp);
            }
            size.sort((o1, o2) -> sizeMap.get(o1).compareTo(sizeMap.get(o2)));
            PriorityQueue<Node> queue = new PriorityQueue<>((o1, o2) -> sizeMap.get(o1).compareTo(sizeMap.get(o2)));

            while(!size.isEmpty()) {
                //Get the node with the lowest search space to start each connected area of the query
                queue.add(size.remove(0));

                //The queue can become empty without us getting a full ordering, so we have it in a bigger while loop
                Iterable<Relationship> edges;
                while(!queue.isEmpty()) {
                    temp = queue.remove();
                    order.add(temp);
                    size.remove(temp);
                    edges = temp.getRelationships();
                    for(Relationship r : edges) {
                        Node other = r.getOtherNode(temp);
                        //Don't add visited nodes (Would be quicker to have a boolean hashmap, but meh)
                        if(!order.contains(other) && !queue.contains(other)) {
                            queue.add(other);
                        }
                    }
                }
            }

            //Backtrack
            SolutionSet mappings = new SolutionSet(1000);
            backtrackSH(0, order, searchSpace, mappings);

            System.out.println("Time Took Naive: " + (t + System.currentTimeMillis()));
            mappings.printSolutions();
            tx.success();
        }
    }

    public void naiveMatchBoost(String queryGraphLbl, String targetGraphLbl) {
        try(Transaction tx = db.beginTx()) {
            long t = -System.currentTimeMillis();
            //Use Neo4j to get search space for each node
            HashMap<Node, ArrayList<Node>> searchSpace = new HashMap<>();

            ResourceIterator<Node> queryGraph = db.findNodes(Label.label(queryGraphLbl));
            Node n;
            ArrayList<Node> search;
            while(queryGraph.hasNext()) {
                n = queryGraph.next();
                search = new ArrayList<>();
                for(Label l: n.getLabels()) {
                    if(l.name().equals(queryGraphLbl)) continue; //At least two labels, we know one, but we need the others
                    //More nodes with secondary label than graph label, so get nodes based on that
                    ResourceIterator<Node> nodes = db.findNodes(Label.label(targetGraphLbl));
                    Iterator<Node> fromNode = new FilteringIterator<>(nodes, node -> node.hasLabel(l));

                    while(fromNode.hasNext()) {
                        search.add(fromNode.next());
                    }
                }
                //Simple optimization
                if(search.size() == 0) {
                    System.out.println("No Solutions");
                    return;
                }
                searchSpace.put(n, search);
            }

            //Pick order
            ArrayList<Node> order = new ArrayList<>(), size = new ArrayList<>();

            Iterator<Node> searchIt = searchSpace.keySet().iterator(); //Randomly sorts keys
            HashMap<Node, Integer> sizeMap = new HashMap<>();
            Node temp;
            while(searchIt.hasNext()) {
                temp = searchIt.next();
                sizeMap.put(temp, searchSpace.get(temp).size());
                size.add(temp);
            }
            size.sort((o1, o2) -> sizeMap.get(o1).compareTo(sizeMap.get(o2)));
            PriorityQueue<Node> queue = new PriorityQueue<>((o1, o2) -> sizeMap.get(o1).compareTo(sizeMap.get(o2)));

            while(!size.isEmpty()) {
                //Get the node with the lowest search space to start each connected area of the query
                queue.add(size.remove(0));

                //The queue can become empty without us getting a full ordering, so we have it in a bigger while loop
                Iterable<Relationship> edges;
                while(!queue.isEmpty()) {
                    temp = queue.remove();
                    order.add(temp);
                    size.remove(temp);
                    edges = temp.getRelationships();
                    for(Relationship r : edges) {
                        Node other = r.getOtherNode(temp);
                        //Don't add visited nodes (Would be quicker to have a boolean hashmap, but meh)
                        if(!order.contains(other) && !queue.contains(other)) {
                            queue.add(other);
                        }
                    }
                }
            }

            //Build drt
            HashMap<Node, DRT> drt = buildDRT(Label.label(queryGraphLbl), searchSpace);

            //Backtrack
            SolutionSet mappings = new SolutionSet(1000);
            subgraphSearch(0, order, searchSpace, drt, mappings);

            System.out.println("Time Took Naive: " + (t + System.currentTimeMillis()));
            mappings.printSolutions();
            tx.success();
        }
    }

    public void graphQLMatch(String queryGraphLbl, String targetGraphLbl) {
        try(Transaction tx = db.beginTx()) {
            long t = -System.currentTimeMillis();
            //Use Neo4j to get search space for each node
            HashMap<Node, ArrayList<Node>> searchSpace = new HashMap<>();

            ResourceIterator<Node> queryGraph = db.findNodes(Label.label(queryGraphLbl));
            Node n;
            ArrayList<Node> search;
            while(queryGraph.hasNext()) {
                n = queryGraph.next();
                search = new ArrayList<>();
                for(Label l: n.getLabels()) {
                    if(l.name().equals(queryGraphLbl)) continue; //At least two labels, we know one, but we need the others
                    //More nodes with secondary label than graph label, so get nodes based on that
                    ResourceIterator<Node> nodes = db.findNodes(Label.label(targetGraphLbl));
                    final Map<String, Object> nProps = n.getAllProperties();
                    Iterator<Node> fromNode = new FilteringIterator<>(nodes, node -> {
                        if(!node.hasLabel(l)) return false;
                        //Profile checking
                        for(String p: nProps.keySet()) {
                            if((int) nProps.get(p) > (int) node.getProperty(p, 0)) return false;
                        }
                        return true;
                    });
                    while(fromNode.hasNext()) {
                        search.add(fromNode.next());
                    }
                }
                //Simple optimization
                if(search.size() == 0) {
                    System.out.println("No Solutions");
                    return;
                }
                searchSpace.put(n, search);
            }

            //Pick order
            ArrayList<Node> order = new ArrayList<>(), size = new ArrayList<>();
            Iterator<Node> searchIt = searchSpace.keySet().iterator(); //Randomly sorts keys
            HashMap<Node, Integer> sizeMap = new HashMap<>();
            HashMap<Node, Integer> reduceFactor = new HashMap<>();
            Node temp;
            while(searchIt.hasNext()) {
                temp = searchIt.next();
                sizeMap.put(temp, searchSpace.get(temp).size());
                size.add(temp);
            }
            size.sort((o1, o2) -> sizeMap.get(o1).compareTo(sizeMap.get(o2)));
            PriorityQueue<Node> queue = new PriorityQueue<>((o1, o2) -> {
                Double s1 = sizeMap.get(o1) * Math.pow(2, -reduceFactor.getOrDefault(o1, 0));
                Double s2 = sizeMap.get(o2) * Math.pow(2, -reduceFactor.getOrDefault(o2, 0));
                return s1.compareTo(s2);
            });

            while(!size.isEmpty()) {
                //Get the node with the lowest search space to start each connected area of the query
                queue.add(size.remove(0));

                //The queue can become empty without us getting a full ordering, so we have it in a bigger while loop
                Iterable<Relationship> edges;
                while(!queue.isEmpty()) {
                    temp = queue.remove();
                    order.add(temp);
                    size.remove(temp);
                    edges = temp.getRelationships();
                    for(Relationship r : edges) {
                        Node other = r.getOtherNode(temp);
                        //Don't add visited nodes (Would be quicker to have a boolean hashmap, but meh)
                        if(!order.contains(other) && !queue.contains(other)) {
                            //Update reduce factor before adding to queue
                            reduceFactor.put(other, reduceFactor.getOrDefault(other, 0)+1);
                            queue.add(other);
                        }
                    }
                }
            }

            //Backtrack
            SolutionSet mappings = new SolutionSet(1000);
            backtrack(0, order, searchSpace, mappings);

            System.out.println("Time Took GraphQL: " + (t + System.currentTimeMillis()));
            mappings.printSolutions();
            tx.success();
        }
    }

    public void graphQLMatchSH(String queryGraphLbl, String targetGraphLbl) {
        try(Transaction tx = db.beginTx()) {
            long t = -System.currentTimeMillis();
            //Use Neo4j to get search space for each node
            HashMap<Node, ArrayList<Node>> searchSpace = new HashMap<>();

            ResourceIterator<Node> queryGraph = db.findNodes(Label.label(queryGraphLbl));
            Node n;
            ArrayList<Node> search;
            while(queryGraph.hasNext()) {
                n = queryGraph.next();
                search = new ArrayList<>();
                for(Label l: n.getLabels()) {
                    if(l.name().equals(queryGraphLbl)) continue; //At least two labels, we know one, but we need the others
                    //More nodes with secondary label than graph label, so get nodes based on that
                    ResourceIterator<Node> nodes = db.findNodes(Label.label(targetGraphLbl));
                    final Map<String, Object> nProps = n.getAllProperties();
                    Iterator<Node> fromNode = new FilteringIterator<>(nodes, node -> {
                        if(!node.hasLabel(l)) return false;
                        //Profile checking
                        for(String p: nProps.keySet()) {
                            if((int) nProps.get(p) > (int) node.getProperty(p, 0)) return false;
                        }
                        return true;
                    });
                    while(fromNode.hasNext()) {
                        search.add(fromNode.next());
                    }
                }
                //Simple optimization
                if(search.size() == 0) {
                    System.out.println("No Solutions");
                    return;
                }
                searchSpace.put(n, search);
            }

            //Pick order
            ArrayList<Node> order = new ArrayList<>(), size = new ArrayList<>();
            Iterator<Node> searchIt = searchSpace.keySet().iterator(); //Randomly sorts keys
            HashMap<Node, Integer> sizeMap = new HashMap<>();
            HashMap<Node, Integer> reduceFactor = new HashMap<>();
            Node temp;
            while(searchIt.hasNext()) {
                temp = searchIt.next();
                sizeMap.put(temp, searchSpace.get(temp).size());
                size.add(temp);
            }
            size.sort((o1, o2) -> sizeMap.get(o1).compareTo(sizeMap.get(o2)));
            PriorityQueue<Node> queue = new PriorityQueue<>((o1, o2) -> {
                Double s1 = sizeMap.get(o1) * Math.pow(2, -reduceFactor.getOrDefault(o1, 0));
                Double s2 = sizeMap.get(o2) * Math.pow(2, -reduceFactor.getOrDefault(o2, 0));
                return s1.compareTo(s2);
            });

            while(!size.isEmpty()) {
                //Get the node with the lowest search space to start each connected area of the query
                queue.add(size.remove(0));

                //The queue can become empty without us getting a full ordering, so we have it in a bigger while loop
                Iterable<Relationship> edges;
                while(!queue.isEmpty()) {
                    temp = queue.remove();
                    order.add(temp);
                    size.remove(temp);
                    edges = temp.getRelationships();
                    for(Relationship r : edges) {
                        Node other = r.getOtherNode(temp);
                        //Don't add visited nodes (Would be quicker to have a boolean hashmap, but meh)
                        if(!order.contains(other) && !queue.contains(other)) {
                            //Update reduce factor before adding to queue
                            reduceFactor.put(other, reduceFactor.getOrDefault(other, 0)+1);
                            queue.add(other);
                        }
                    }
                }
            }

            //Backtrack
            SolutionSet mappings = new SolutionSet(1000);
            backtrackSH(0, order, searchSpace, mappings);

            System.out.println("Time Took GraphQL: " + (t + System.currentTimeMillis()));
            mappings.printSolutions();
            tx.success();
        }
    }

    public void graphQLMatchBoost(String queryGraphLbl, String targetGraphLbl) {
        try(Transaction tx = db.beginTx()) {
            long t = -System.currentTimeMillis();
            //Use Neo4j to get search space for each node
            HashMap<Node, ArrayList<Node>> searchSpace = new HashMap<>();

            ResourceIterator<Node> queryGraph = db.findNodes(Label.label(queryGraphLbl));
            Node n;
            ArrayList<Node> search;
            while(queryGraph.hasNext()) {
                n = queryGraph.next();
                search = new ArrayList<>();
                for(Label l: n.getLabels()) {
                    if(l.name().equals(queryGraphLbl)) continue; //At least two labels, we know one, but we need the others
                    //More nodes with secondary label than graph label, so get nodes based on that
                    ResourceIterator<Node> nodes = db.findNodes(Label.label(targetGraphLbl));
                    final Map<String, Object> nProps = n.getAllProperties();
                    Iterator<Node> fromNode = new FilteringIterator<>(nodes, node -> {
                        if(!node.hasLabel(l)) return false;
                        //Profile checking
                        for(String p: nProps.keySet()) {
                            if((int) nProps.get(p) > (int) node.getProperty(p, 0)) return false;
                        }
                        return true;
                    });
                    while(fromNode.hasNext()) {
                        search.add(fromNode.next());
                    }
                }
                //Simple optimization
                if(search.size() == 0) {
                    System.out.println("No Solutions");
                    return;
                }
                searchSpace.put(n, search);
            }

            //Pick order
            ArrayList<Node> order = new ArrayList<>(), size = new ArrayList<>();
            Iterator<Node> searchIt = searchSpace.keySet().iterator(); //Randomly sorts keys
            HashMap<Node, Integer> sizeMap = new HashMap<>();
            HashMap<Node, Integer> reduceFactor = new HashMap<>();
            Node temp;
            while(searchIt.hasNext()) {
                temp = searchIt.next();
                sizeMap.put(temp, searchSpace.get(temp).size());
                size.add(temp);
            }
            size.sort((o1, o2) -> sizeMap.get(o1).compareTo(sizeMap.get(o2)));
            PriorityQueue<Node> queue = new PriorityQueue<>((o1, o2) -> {
                Double s1 = sizeMap.get(o1) * Math.pow(2, -reduceFactor.getOrDefault(o1, 0));
                Double s2 = sizeMap.get(o2) * Math.pow(2, -reduceFactor.getOrDefault(o2, 0));
                return s1.compareTo(s2);
            });

            while(!size.isEmpty()) {
                //Get the node with the lowest search space to start each connected area of the query
                queue.add(size.remove(0));

                //The queue can become empty without us getting a full ordering, so we have it in a bigger while loop
                Iterable<Relationship> edges;
                while(!queue.isEmpty()) {
                    temp = queue.remove();
                    order.add(temp);
                    size.remove(temp);
                    edges = temp.getRelationships();
                    for(Relationship r : edges) {
                        Node other = r.getOtherNode(temp);
                        //Don't add visited nodes (Would be quicker to have a boolean hashmap, but meh)
                        if(!order.contains(other) && !queue.contains(other)) {
                            //Update reduce factor before adding to queue
                            reduceFactor.put(other, reduceFactor.getOrDefault(other, 0)+1);
                            queue.add(other);
                        }
                    }
                }
            }

            //Build drt
            HashMap<Node, DRT> drt = buildDRT(Label.label(queryGraphLbl), searchSpace);

            //Backtrack
            SolutionSet mappings = new SolutionSet(1000);
            subgraphSearch(0, order, searchSpace, drt, mappings);

            System.out.println("Time Took GraphQL: " + (t + System.currentTimeMillis()));
            mappings.printSolutions();
            tx.success();
        }
    }

    private boolean backtrack(int i, ArrayList<Node> order, HashMap<Node, ArrayList<Node>> searchSpace,
                              SolutionSet mappings) {
        if(mappings.isFilled()) return false;
        if(i == order.size()) return true;
        Node curr = order.get(i);
        ArrayList<Node> space = searchSpace.get(curr);

        for(Node v: space) {
            if(mappings.inSolution(v) || !check(v, i, order, mappings)) continue;
            mappings.put(curr, v);
            if(backtrack(i+1, order, searchSpace, mappings)) {
                mappings.nextSolution();
                HashMap<Node, Node> prev = mappings.prevSolution();
                for(int j = 0; j < i; j++) {
                    mappings.put(order.get(j), prev.get(order.get(j)));
                }
            }
        }
        return false;
    }

    private boolean backtrackSH(int i, ArrayList<Node> order, HashMap<Node, ArrayList<Node>> searchSpace,
                                SolutionSet mappings) {
        if(mappings.isFilled()) return false;
        if(i == order.size()) return true;
        Node curr = order.get(i);
        ArrayList<Node> space = searchSpace.get(curr);

        for(Node v: space) {
            if(mappings.inSolution(v) || !check(v, i, order, mappings)) continue;
            mappings.put(curr, v);
            if(backtrack(i+1, order, searchSpace, mappings)) {
                mappings.nextSolution();
                HashMap<Node, Node> prev = mappings.prevSolution(), temp = new HashMap<>(prev);
                Collection<Long> nodesUsed = temp.values().stream().map(Node::getId)
                        .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
                List<Set<Long>> newMappings = temp.values().parallelStream().map(node -> {
                    Set<Long> possValues = new HashSet<>(Arrays.asList((Long[])v.getProperty("hyper_SEC")));
                    for(Long l: possValues) {
                        if(nodesUsed.contains(l)) {
                            possValues.remove(l);
                        }
                    }
                    //Guarantee one value in set
                    possValues.add(node.getId());
                    return possValues;
                }).collect(Collectors.toList());

                int[] indList = new int[newMappings.size()];
                Arrays.fill(indList, 0);
                int k=0;
                Set<Long> tempSet;
                while(indList[0] < newMappings.get(0).size()) {
                    if(k == newMappings.size()) {
                        //Commit mapping
                        mappings.nextSolution();
                        k--;
                    }
                    tempSet = newMappings.get(k);
                    if(indList[k] < tempSet.size()) {
                        //Add to new mapping
                        k++;
                    } else {
                        indList[k] = 0;
                        k--;
                    }
                }

                for(int j = 0; j < i; j++) {
                    mappings.put(order.get(j), prev.get(order.get(j)));
                }
            }
        }
        return false;
    }

    private boolean subgraphSearch(int i, ArrayList<Node> order, HashMap<Node, ArrayList<Node>> searchSpace,
                                HashMap<Node, DRT> drt, SolutionSet mappings) {
        if(mappings.isFilled()) return false;

        boolean flag = false;

        if(i == order.size()) {
            mappings.nextSolution();
            HashMap<Node, Node> prev = mappings.prevSolution(), temp = new HashMap<>(prev);
            Node curr = prev.get(order.get(i-1));

            Collection<Long> nodesUsed = temp.values().stream().map(Node::getId)
                    .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
            List<Set<Long>> newMappings = temp.values().parallelStream().map(node -> {
                Set<Long> possValues = new HashSet<>(Arrays.asList((Long[])curr.getProperty("hyper_SEC")));
                for(Long l: possValues) {
                    if(nodesUsed.contains(l)) {
                        possValues.remove(l);
                    }
                }
                //Guarantee one value in set
                possValues.add(node.getId());
                return possValues;
            }).collect(Collectors.toList());

            int[] indList = new int[newMappings.size()];
            Arrays.fill(indList, 0);
            int k=0;
            Set<Long> tempSet;
            while(indList[0] < newMappings.get(0).size()) {
                if(k == newMappings.size()) {
                    //Commit mapping
                    mappings.nextSolution();
                    k--;
                }
                tempSet = newMappings.get(k);
                if(indList[k] < tempSet.size()) {
                    //Add to new mapping
                    k++;
                } else {
                    indList[k] = 0;
                    k--;
                }
            }
            dynamicCL(searchSpace.get(curr), curr);
            return true;
        }

        Node curr = order.get(i);
        ArrayList<Node> space = searchSpace.get(curr);

        for(Node v: space) {
            if(mappings.inSolution(v) || !check(v, i, order, mappings)) continue;
            mappings.put(curr, v);
            if(subgraphSearch(i+1, order, searchSpace, drt, mappings)) {
                HashMap<Node, Node> prev = mappings.prevSolution();
                for(int j = 0; j < i; j++) {
                    mappings.put(order.get(j), prev.get(order.get(j)));
                }
                flag = true;
            }
        }
        if(flag) {
            Node prevNode = order.get(i-1);
            dynamicCL(searchSpace.get(prevNode), mappings.get(prevNode));
        }

        searchSpace.put(curr, space);
        return flag;
    }

    private ArrayList<Node> dynamicCL(ArrayList<Node> space, Node curr) {
        ArrayList<Node> ret = new ArrayList<>(space);
        db.execute("MATCH (u)-[:hyper_SC*]->(v) " +
                "WHERE id(u)=" + curr.getId() +" " +
                "RETURN DISTINCT id(v), p AS Path, LENGTH(p) AS PathSize " +
                "ORDER BY PathSize"
        ).stream().<Node>map(n -> db.getNodeById((Long) n.get("id(v)"))).forEach(ret::add);
        return ret;
    }

    private HashMap<Node, DRT> buildDRT(Label queryGraphLabel, HashMap<Node, ArrayList<Node>> searchSpace) {
        HashMap<Node, DRT> ret = new HashMap<>();
        ArrayList<Node> queryGraph = db.findNodes(queryGraphLabel).stream().collect(Collectors.toCollection(ArrayList::new));
        for(Node u: queryGraph) {
            DRT drt = new DRT(u);
            ArrayList<Node> candidateList = searchSpace.get(u);
            //BuildDRT method from paper
            for(int i=0; i<candidateList.size(); i++) {
                for(int j = i+1; j<candidateList.size(); j++) {
                    Node h_i = candidateList.get(i);
                    Node h_j = candidateList.get(j);
                    if(QDE(u, h_i, h_j)) {
                        candidateList.remove(h_j);
                        drt.addQDE(h_i, h_j);
                        j--;
                    }
                }
            }
            for(int i=0; i<candidateList.size(); i++) {
                for(int j = i + 1; j < candidateList.size(); j++) {
                    Node h_i = candidateList.get(i);
                    Node h_j = candidateList.get(j);
                    if(QDC(u, h_i, h_j)) {
                        drt.addQDCChild(h_i, h_j);
                        drt.addQDCParent(h_j);
                    } else if(QDC(u, h_j, h_i)) {
                        drt.addQDCChild(h_j, h_i);
                        drt.addQDCParent(h_i);
                    }
                }
            }
            ret.put(u, drt);
        }
        return ret;
    }

    // adj(h_i) - {h_j} > adj(h_j) - {h_i}
    private boolean QDC(Node u, Node h_i, Node h_j) {
        String labelString = StreamSupport.stream(u.getPropertyKeys().spliterator(), false).filter(name -> name.startsWith("profile_"))
                .map(name -> name.substring(8)).reduce("", (a,b) -> a + "|:" + b);
        Set<Long> setH_i = db.execute("MATCH (u)--(v" + labelString.substring(1) +  ") " +
                "WHERE id(u)=" + h_i.getId() + " " +
                "RETURN id(v)").stream().map(node -> (Long)node.get("id(v)")).collect(Collectors.toSet());
        Set<Long> setH_j = db.execute("MATCH (u)--(v" + labelString.substring(1) +  ") " +
                "WHERE id(u)=" + h_j.getId() + " " +
                "RETURN id(v)").stream().map(node -> (Long)node.get("id(v)")).collect(Collectors.toSet());

        setH_i.remove(h_j.getId());
        setH_j.remove(h_i.getId());
        return setH_i.containsAll(setH_j);
    }

    // adj(h_i) - {h_j} = adj(h_j) - {h_i}
    private boolean QDE(Node u, Node h_i, Node h_j) {
        String labelString = StreamSupport.stream(u.getPropertyKeys().spliterator(), false).filter(name -> name.startsWith("profile_"))
                .map(name -> name.substring(8)).reduce("", (a,b) -> a + "|:" + b);
        Set<Long> setH_i = db.execute("MATCH (u)--(v" + labelString.substring(1) +  ") " +
                "WHERE id(u)=" + h_i.getId() + " " +
                "RETURN id(v)").stream().map(node -> (Long)node.get("id(v)")).collect(Collectors.toSet());
        Set<Long> setH_j = db.execute("MATCH (u)--(v" + labelString.substring(1) +  ") " +
                "WHERE id(u)=" + h_j.getId() + " " +
                "RETURN id(v)").stream().map(node -> (Long)node.get("id(v)")).collect(Collectors.toSet());

        setH_i.remove(h_j.getId());
        setH_j.remove(h_i.getId());
        return setH_i.containsAll(setH_j) && setH_j.containsAll(setH_i);
    }

    private boolean check(Node v, int i, ArrayList<Node> order, SolutionSet mappings) {
        Node u, mapU, curr = order.get(i);
        for(int j = 0; j < i; j++) {
            u = order.get(j);
            for(Relationship r: u.getRelationships()) {
                if(r.getOtherNode(u).equals(curr)) {
                    mapU = mappings.get(u);
                    boolean found = false;
                    for(Relationship mapR: mapU.getRelationships()) {
                        if(mapR.getOtherNode(mapU).equals(v)) {
                            found = true;
                            break;
                        }
                    }
                    if(found) break;
                    else return false;
                }
            }
        }
        return true;
    }

    public void close() {
        System.out.println("----Closing Neo4j Connection----");
        if(db != null) db.shutdown();
    }

    private class SolutionSet {
        private int index, max;
        private ArrayList<HashMap<Node, Node>> mappings;

        public SolutionSet(int max) {
            this.max = max;
            this.index = 0;
            this.mappings = new ArrayList<>(max);
            mappings.add(new HashMap<>());
        }

        public void put(Node k, Node v) {
            mappings.get(index).put(k, v);
        }

        public Node get(Node k) {
            return mappings.get(index).get(k);
        }

        public boolean isFilled() {
            return index >= max;
        }

        public boolean inSolution(Node v) {
            return mappings.get(index).values().contains(v);
        }

        public void nextSolution() {
            index++;
            mappings.add(index, new HashMap<>());
        }

        public HashMap<Node, Node> prevSolution() {
            return mappings.get(index-1);
        }

        public void printSolutions() {
            if(index == 0) {
                System.out.println("No Solutions");
            } else {
                while(index > 0) {
                    index--;
                    HashMap<Node, Node> curr = mappings.get(index);
                    for(Node k: curr.keySet()) {
                        System.out.print(k.getId() + "," + curr.get(k).getId()+";");
                    }
                    System.out.println();
                }
            }
        }
    }

    private class DRT {
        Node queryNode;
        ArrayList<Node> nodes;
        HashMap<Node, ArrayList<Node>> qdc_children;
        HashMap<Node, Integer> qdc_parents;
        HashMap<Node, ArrayList<Node>> qde_list;

        public DRT(Node queryNode) {
            this.queryNode = queryNode;
            nodes = new ArrayList<>();
            qdc_children = new HashMap<>();
            qdc_parents = new HashMap<>();
            qde_list = new HashMap<>();
        }

        public void newNode(Node dataNode) {
            nodes.add(dataNode);
            qdc_children.put(dataNode, new ArrayList<>());
            qdc_parents.put(dataNode, 0);
            qde_list.put(dataNode, new ArrayList<>());
        }

        public void addQDCChild(Node key, Node node) {
            if(!nodes.contains(key)) newNode(key);
            ArrayList<Node> value = qdc_children.get(key);
            value.add(node);
            qdc_children.put(key, value);
        }

        public void addQDCParent(Node key) {
            if(!nodes.contains(key)) newNode(key);
            qdc_parents.put(key, qdc_parents.get(key)+1);
        }

        public void addQDE(Node key, Node node) {
            if(!nodes.contains(key)) newNode(key);
            ArrayList<Node> value = qde_list.get(key);
            value.add(node);
            qde_list.put(key, value);
        }

        public ArrayList<Node> getNodes() {
            return nodes;
        }
    }
}
