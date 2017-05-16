import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Author: Nate
 * Created on: 4/4/17
 * Description: Loader for Project
 */
public class Neo4jLoader {
    /* Data sets from:
     * WordNet - http://vlado.fmf.uni-lj.si/pub/networks/data/dic/Wordnet/Wordnet.htm
     * Human, Yeast - GraphQL Homework
     * Email - https://snap.stanford.edu/data/email-Enron.html
     * DBLP - https://snap.stanford.edu/data/com-DBLP.html
     * Youtube - https://snap.stanford.edu/data/com-Youtube.html
     */

    public static void main(String[] args) {
        if(args.length == 1) {
            Neo4jLoader loader = new Neo4jLoader(args[0]);
            Scanner in = new Scanner(System.in);
            String line;
            String[] parts;
            File temp;
            while(in.hasNextLine()) {
                line = in.nextLine();
                parts = line.split(" ");
                if(parts[0].equals("exit")) break;
                temp = new File(parts[0]);
                if(!temp.exists()) {
                    System.out.println("File does not exist");
                    continue;
                }
                switch(parts[1].toLowerCase()) {
                    case "igraph":
                    case "1":
                        loader.loadiGraph(temp);
                        break;
                    case "net":
                    case "2":
                        loader.loadWordNet(temp);
                        break;
                    case "snap":
                    case "3":
                        loader.loadSnap(temp);
                        break;
                }
            }
            loader.close();
        } else usage();
    }

    private static void usage() {
        throw new IllegalArgumentException("usage: java Neo4jLoader neo4jUrl");
    }

    private BatchInserter bi;

    public Neo4jLoader(String neoPath) {
        try {
            System.out.println("----Opening Neo4j Connection----");
            File neoDir = new File(neoPath);
            bi = BatchInserters.inserter(neoDir);
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void loadiGraph(File f) {
        try(Scanner scan = new Scanner(f)) {
            System.out.println("--Loading iGraph--");
            String name = f.getName().split("\\.")[0], line;
            Label curGraph = null;
            String[] parts;
            HashMap<String, Long> mapping = new HashMap<>();
            while(scan.hasNextLine()) {
                line = scan.nextLine();
                parts = line.split(" ");
                switch(parts[0]) {
                    case "t":
                        //new graph
                        curGraph = Label.label(name+"_"+parts[2]);
                        mapping = new HashMap<>();
                        break;
                    case "v":
                        //new vertex
                        int size = parts.length;
                        Label[] labels = new Label[size-1];
                        labels[0] = curGraph;
                        for(int i=2; i<size; i++) {
                            labels[i-1] = Label.label(parts[i]);
                        }
                        long temp = bi.createNode(new HashMap<>(), labels);
                        mapping.put(parts[1], temp);
                        break;
                    case "e":
                        //new edge
                        long n1 = mapping.get(parts[1]);
                        long n2 = mapping.get(parts[2]);
                        bi.createRelationship(n1, n2, RelationshipType.withName(parts[3]), new HashMap<>());
                        //update profiles
                        Map<String, Object> n1Prop = bi.getNodeProperties(n1);
                        Map<String, Object> n2Prop = bi.getNodeProperties(n2);
                        for(Label l: bi.getNodeLabels(n1)) {
                            if(curGraph != null && l.name().equals(curGraph.name())) continue;
                            bi.setNodeProperty(n2, "profile_"+l.name(),
                                    ((int) n2Prop.getOrDefault("profile_"+l.name(), 0)) + 1);
                        }
                        for(Label l: bi.getNodeLabels(n2)) {
                            if(curGraph != null && l.name().equals(curGraph.name())) continue;
                            bi.setNodeProperty(n1, "profile_"+l.name(),
                                    ((int) n1Prop.getOrDefault("profile_"+l.name(), 0)) + 1);
                        }
                        break;
                }
            }
            System.out.println("--Loaded iGraph--");
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void loadWordNet(File f) {
        File verts = new File(f.getPath() + "/wordnet3.net");
        File labels = new File(f.getPath() + "/wordnet3.clu");
        try(Scanner graph = new Scanner(verts); Scanner label = new Scanner(labels)) {
            System.out.println("--Loading WordNet--");
            String line;
            String[] parts;
            Label curGraph = Label.label("WordNet"), curLabel;
            RelationshipType edge = null;
            HashMap<String, Long> mapping = new HashMap<>();
            HashMap<String, Object> prop = new HashMap<>();
            boolean vertMode = true;
            while(graph.hasNextLine()) {
                line = graph.nextLine();
                parts = line.split(" ");
                if(parts[0].equals("*Arcslist")) {
                    vertMode = false;
                    edge = RelationshipType.withName(parts[1].substring(1,parts[1].length()-1));
                    prop.clear();
                    continue;
                } else if(parts[0].equals("*Vertices")) {
                    //Do I need N?
                    vertMode = true;
                    //Consume label first line
                    label.nextLine();
                    continue;
                }

                if(vertMode) {
                    prop.put("word", parts[1].substring(1, parts[1].length()));
                    curLabel = Label.label(label.nextLine().trim());
                    mapping.put(parts[0], bi.createNode(prop, curGraph, curLabel));
                } else {
                    long n1 = mapping.get(parts[0]), n2;
                    for(int i = 1; i < parts.length; i++) {
                        n2 = mapping.get(parts[i]);
                        bi.createRelationship(n1, n2, edge, prop);
                        //update profiles
                        Map<String, Object> n1Prop = bi.getNodeProperties(n1);
                        Map<String, Object> n2Prop = bi.getNodeProperties(n2);
                        for(Label l: bi.getNodeLabels(n1)) {
                            if(l.name().equals(curGraph.name())) continue;
                            bi.setNodeProperty(n2, "profile_"+l.name(),
                                    ((int) n2Prop.getOrDefault("profile_"+l.name(), 0)) + 1);
                        }
                        for(Label l: bi.getNodeLabels(n2)) {
                            if(l.name().equals(curGraph.name())) continue;
                            bi.setNodeProperty(n1, "profile_"+l.name(),
                                    ((int) n1Prop.getOrDefault("profile_"+l.name(), 0)) + 1);
                        }
                    }
                }
            }
            System.out.println("--Loaded WordNet--");
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void loadSnap(File f) {
        String name = f.getName().split("\\.")[0];
        File cmtys = new File(f.getParent() + "/" + name + ".all.cmty.txt");
        System.out.println("--Loading SNAP--");
        String line;
        String[] parts;
        Label curGraph = Label.label(name), curLabel;
        HashMap<String, ArrayList<Label>> communities = new HashMap<>();
        if(cmtys.exists()) {
            try(Scanner cmty = new Scanner(cmtys)) {
                ArrayList<Label> tempLabels;
                int cnt = 0;
                while(cmty.hasNextLine()) {
                    line = cmty.nextLine();
                    parts = line.split("\\s");
                    for(String part : parts) {
                        curLabel = Label.label("" + cnt);
                        if(communities.containsKey(part)) {
                            tempLabels = communities.get(part);
                            tempLabels.add(curLabel);
                            communities.put(part, tempLabels);
                        } else {
                            tempLabels = new ArrayList<>();
                            tempLabels.add(curGraph);
                            communities.put(part, tempLabels);
                        }
                    }
                }
            } catch(FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        try(Scanner edge = new Scanner(f)) {
            HashMap<String, Long> mapping = new HashMap<>();
            HashMap<String, Object> prop = new HashMap<>();
            long n1, n2;
            while(edge.hasNextLine()) {
                line = edge.nextLine();
                if(line.startsWith("#")) continue;
                parts = line.split("\\s");
                //Make sure we don't have duplicates
                if(Integer.parseInt(parts[0]) > Integer.parseInt(parts[1])) continue;
                if(!mapping.containsKey(parts[0])) {
                    Label[] labels;
                    if(communities.containsKey(parts[0])) {
                        ArrayList<Label> temp = communities.get(parts[0]);
                        labels = new Label[temp.size()];
                        temp.toArray(labels);
                    }
                    else labels = new Label[]{curGraph};
                    mapping.put(parts[0], bi.createNode(prop, labels));
                }
                if(!mapping.containsKey(parts[1])) {
                    Label[] labels;
                    if(communities.containsKey(parts[1])) {
                        ArrayList<Label> temp = communities.get(parts[1]);
                        labels = new Label[temp.size()];
                        temp.toArray(labels);
                    }
                    else labels = new Label[]{curGraph};
                    mapping.put(parts[1], bi.createNode(prop, labels));
                }
                n1 = mapping.get(parts[0]);
                n2 = mapping.get(parts[1]);
                bi.createRelationship(n1, n2, RelationshipType.withName(""), prop);
                //update profiles
                Map<String, Object> n1Prop = bi.getNodeProperties(n1);
                Map<String, Object> n2Prop = bi.getNodeProperties(n2);
                for(Label l: bi.getNodeLabels(n1)) {
                    if(l.name().equals(curGraph.name())) continue;
                    bi.setNodeProperty(n2, "profile_"+l.name(),
                            ((int) n2Prop.getOrDefault("profile_"+l.name(), 0)) + 1);
                }
                for(Label l: bi.getNodeLabels(n2)) {
                    if(l.name().equals(curGraph.name())) continue;
                    bi.setNodeProperty(n1, "profile_"+l.name(),
                            ((int) n1Prop.getOrDefault("profile_"+l.name(), 0)) + 1);
                }
            }
            System.out.println("--Loaded SNAP--");
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        System.out.println("----Closing Neo4j Connection----");
        if(bi != null) bi.shutdown();
    }
}
