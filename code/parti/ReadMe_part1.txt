
IDE Required: Eclipse

Steps:

1) Import Part1 as a existing maven project in eclipse.

2) Include pom.xml and Part1-Part1 jar file while extracting.

3) Run pom.xml as a Maven project to generate the jar file again or copy the Part1-Part1.jar into cluster
   Note: Part1-Part1.jar can be  found in axm163631_lxp160730_assignment1b/parti/Part1/target

4)The positive words and negative words files are uploaded manually to the hdfs initially.

5) Execute the jar file using below command in cluster

hadoop jar Part1-Part1.jar Assignment2.Part1.Part1 hdfs://cshadoop1/user/lxp160730/assignment1/ hdfs://cshadoop1/user/lxp160730/assignment1b_results

Note: Desination folder should not exist in the path prior to running  the command.

6) Output can be verified by executing below command:

hdfs dfs -cat /user/lxp160730/assignment1b_results/part-r-00000 

Output:
Total negative Words:  15007
Total positive Words : 18246

