#Files included
-> Assignment2 maven project



#Steps to run
-> Directly  eclipse maven project can be imported into eclipse.
-> Once imported, run it as maven install. Then, yelp.jar is created.
-> Copy the jar file in to the local machine which had hadoop cluster.

Or else I have included yelp.jar file. Use that to run the commands.

-> Now you can run the jar file using following commands

#Question1:
-> hadoop jar yelp.jar Question1 /input file path/ /output folder path/ 
Ex:hadoop jar yelp.jar Question1 /yelp/business/business.csv /user/sxs155933/Assignment2/Outputs/


#Question2: (has two files)
-> hadoop jar yelp.jar Question2 /input file path/ /output folder path/ (outputs 'business_id' as 'KEY' 'full_address' as 'VALUE')
Ex:hadoop jar yelp.jar Question2 /yelp/business/business.csv /user/sxs155933/Assignment2/Question2




#Question3
-> hadoop jar yelp.jar Question3 /input file path/ /output folder path/ 

Ex:hadoop jar yelp.jar Question3 /yelp/business/business.csv /user/sxs155933/Assignment2/Question3

#Question4
-> hadoop jar yelp.jar Question4 /input file path/ /output folder path/ 
Ex:
hadoop jar yelp.jar Question4 /yelp/review/review.csv /user/sxs155933/Assignment2/Question4

#Question5
-> hadoop jar yelp.jar Question5 /input file path/ /output folder path/ 
Ex:
hadoop jar yelp.jar Question5 /yelp/review/review.csv /user/sxs155933/Assignment2/Question5