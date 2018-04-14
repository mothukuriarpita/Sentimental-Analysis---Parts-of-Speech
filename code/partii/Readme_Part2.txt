IDE Required: Eclipse

1) Import POS folder(which is present in Partii folder) as a existing maven project in eclipse.

2) Include pom.xml and POS jar file while extracting.

3) Run pom.xml as a Maven project to generate the jar file again or copy the POS.jar into cluster
          Note: POS-1.jar can be  found in axm163631_lxp160730_assignment1b/Partii/POS/target

4) Execute the jar file using below command in cluster(In the below command axm163631 is the 
netid."hdfs://cshadoop1/user/axm163631/assignment1/text.txt" is the input path of text file ,"hdfs://cshadoop1/user/axm163631/assignment1/pos" is the destination path where files will be saved):

Note: Desination folder should not exist in the path prior to running  the command. 

hadoop jar POS-1.jar Assignment1b.POS.POS hdfs://cshadoop1/user/axm163631/assignment1/text.txt hdfs://cshadoop1/user/axm163631/assignment1/pos

Note: POS words files is downloaded from "http://www.dcs.shef.ac.uk/research/ilash/Moby/mpos.tar.Z" and uploaded manually into hdfs location /user/axm163631/assignment1/pos_words.txt

5) Output can be verified by executing below command:

hdfs dfs -ls /user/axm163631/assignment1/pos
hdfs dfs -cat /user/axm163631/assignment1/pos/part-r-00000
