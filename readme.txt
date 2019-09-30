Big Data and Analytics 		CS 6350 
Name: Maleeha Koul	 	Netid: MSK180001

Setup The hadoop according to the commands given below:
1. Stop the Hdfs if already running
	stop-dfs.sh
2. Format the namenode
	hdfs namenode-format
3. Start the server
	start-dfs.sh
4. Make the input directory
	hdfs dfs -mkdir /maleeha
	hdfs dfs -mkdir /maleeha/input
5. Upload the input datafile
	hdfs dfs -put ~/Desktop/soc-LiveJournal1Adj.txt /maleeha/input
	hdfs dfs -put ~/Desktop/userdata.txt /maleeha/input
6. Check the file
	hdfs dfs -ls -R /
===========================================================================================================================================
Question 1
============================================================================================================================================
 Package name: src
 Class name  : MutualFriend.java
 Jar name    : mutualFriends.jar
 

(1) Log into HDFS

(2) Run the following command
	hadoop jar ~/Desktop/mutualFriends.jar MutualFriend /maleeha/input/soc-LiveJournal1Adj.txt /maleeha/output

(3)To display the contents of an output file
	hdfs dfs -get /maleeha/output
	cat part-r-00000
	
	hdfs dfs -cat /maleeha/output/part-r-00000 | grep "0,1<CTRL+V<TAB>>"
	hdfs dfs -cat /maleeha/output/part-r-00000 | grep "20,28193<CTRL+V<TAB>>"
	hdfs dfs -cat /maleeha/output/part-r-00000 | grep "1,29826<CTRL+V<TAB>>"
	hdfs dfs -cat /maleeha/output/part-r-00000 | grep "6222,19272<CTRL+V<TAB>>"
	hdfs dfs -cat /maleeha/output/part-r-00000 | grep "28041,28056<CTRL+V<TAB>>"


=============================================================================================================================================
Question 2
=============================================================================================================================================
 Package name: src
 Class name  : Top10.java
 Jar name    : top10.jar

(1) Log into HDFS

(2) Run the following command
	hadoop jar ~/Desktop/top10.jar Top10 /maleeha/input/soc-LiveJournal1Adj.txt /maleeha/output1 /maleeha/output2

(3)To display the contents of an output file
	
	hdfs dfs -cat /maleeha/output2/part-r-00000 

=========================================================================================================================================================================
Question 3
========================================================================================================================================================================
 Package name: src
 Class name  : Join.java
 Jar name    : join.jar

 (1) Log into HDFS

 (2) Run the following command
     
	hadoop jar ~/Desktop/join.jar Join /maleeha/input/soc-LiveJournal1Adj.txt /maleeha/input/userdata.txt /maleeha/output

  
  (3)To display the contents of an output file
    
	hdfs dfs -get /maleeha/output
	cat part-r-00000

	hdfs dfs -cat /maleeha/output/part-r-00000 | grep "0,1  "
	hdfs dfs -cat /maleeha/output/part-r-00000 | grep "20,28193     "
	hdfs dfs -cat /maleeha/output/part-r-00000 | grep "1,29826      "
	hdfs dfs -cat /maleeha/output/part-r-00000 | grep "6222,19272   "
	hdfs dfs -cat /maleeha/output/part-r-00000 | grep "28041,28056  "


==============================================================================================================================
Question 4
==============================================================================================================================
 Package name: src
 Class name  : AverageAge.java
 Jar name    : averageAge.jar

 (1) Log into HDFS

 (2) Run the following command
	
	hadoop jar ~/Desktop/averageAge.jar AverageAge /maleeha/input/soc-LiveJournal1Adj.txt /maleeha/output1 /maleeha/input/userdata.txt /maleeha/output2

 (3)To display the contents of an output file
	hdfs dfs -get /maleeha/output1
	hdfs dfs -get /maleeha/output2
    
	hdfs dfs -cat /maleeha/output2/part-r-00000

