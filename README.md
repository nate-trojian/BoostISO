                            Boost ISO Implementation		
                Nathaniel Trojian (njt9982)	Siddharth Subramanian (ss6813)
                        Advisor : Carlos R Rivero 
--------------------------------------------------------------------------------------

This project is an implementation of the Boost ISO algorithm based on the paper "Exploiting vertex relationships in speeding up subgraph isomorphism over large graphs" by Ren X. and Wang J.
The project requires Java 8 and neo4j 2.0+  

The project uses datasets which can be obtained from the following links  
     * WordNet - http://vlado.fmf.uni-lj.si/pub/networks/data/dic/Wordnet/Wordnet.htm  
     * Human, Yeast - GraphQL Homework
     
There are two java files : 

    * Neo4jLoader.java

        * This file loads the above datasets into the neo4j database. You can run the program by first compiling it using the following     command : javac Neo4jLoader.java

        * Run this file with the desired path to a new Neo4j database:
            command : java Neo4jLoader path

        * Then run this file against the dataset of our choice by using the following commands : 
            Human, Yeast dataset -
                path_to_file igraph    
            WordNet dataset -
                path_to_file net

        * To exit the program : 
                command : exit

    * BoostIso.java  
        * This file implements the Boost ISO algorithm on the above datasets. You can run the program by first compiling it using the following command : javac BoostIso.java

        * We can then run this file against the dataset of our choice by using the following commands : 
            Human, Yeast dataset -
                java BoostIso igraph    
            WordNet dataset -
                java BoostIso net
    
    * ProjectLoader   
        This file specifies the usage of each of the datasets for the loader program.
