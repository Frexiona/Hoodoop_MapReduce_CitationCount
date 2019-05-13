# Hoodoop_MapReduce_CitationCount
Citation count including direct citations and indirect citations

How to run this ?

Step 1: hadoop com.sun.tools.javac.Main CitationCount.java

Step 2: jar cf cc.jar CitationCount*.class

Step 3: hadoop jar cc.jar CitationCount /corpus_name /output1 /output2
