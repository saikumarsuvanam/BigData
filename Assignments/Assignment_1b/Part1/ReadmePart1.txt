Part 1:
We have included the maven project and jar file also.
using maven project:
1) import maven project
2)clean and install
3)Now move the jar file to local machine containing hdfs.
4)All the 6 input files, positive words and negative words should be in hdfs.
5)use following commands to check on all links given in question
    
Note:input folder path contains all the 6 files.

Command:hadoop jar WordCount.jar WordCount.WordCount.Question1  <INPUTFOLDERPATH>  <OUTPUTPATH>  <POSITIVEWORDSPATH> <NEGATIVEWORDSPATH> 

Ex:hadoop jar WordCount.jar WordCount.WordCount.Question1 /user/sxs155933/Assignment1/Part1  /user/sxs155933/Assignment1b/output.txt  /user/sxs155933/positive-words.txt  /user/sxs155933/negative-words.txt  






