-----------------------------------------------------------README file---------------------------------------------------------------

- Raya Wattad ID:324883388
- Ameen Amer ID:325731990

-------------------------------How to run our project ---------------------------------------

1- Download the zip file

2- Run the command:
cd Hadoop_Project/Hadoop

Note: we have already uploaded two jar folders - "jars1" holds the jars that include steps with local aggregation, and "jars2" holds the jars without.

Important Note: The jars that we uploaded are defined to take as input the Google's 3gram, so if you want to run the program on a different input you should change the FileInputFormat and the FileOutputFormat of each Step accordingly, as well as re-compiling the Steps and uploading them again to S3. 

4- Run the command:
mvn clean package 

5- Run the cmd: 
java -jar target/App.jar <"jars1"/"jars2"> <the output folder name on S3> <number of instances>.

---------------------------MapReduce Workflow Explanation------------------------
The workflow of our map-reduce program is as follows:
 
-----------------------------------------------------------

Step 1: Data Preparation

Mapper:
For each trigram in the corpus (w1 w2 w3 count):
Emit intermediate keys and counts to aggregate data for different analyses:
emit(w2:*, count)
emit(w3:*, count)
emit(w1 w2:*, count)
emit(w2 w3:*, count)
emit(C0:*, count)
emit(w1 w2 w3, count)
Emit intermediate keys with trigrams for context:
emit(w2, (w1, w2, w3))
emit(w3, (w1, w2, w3))
emit(w1 w2, (w1, w2, w3))
emit(w2 w3, (w1, w2, w3))
emit(C0, (w1, w2, w3))


Comparator:
Sort the keys such that keys ending with :* come before their corresponding base keys. This ensures proper grouping for processing.



Reducer:
For each ngram:* key:
Sum all counts associated with it and store the total in a global variable (lastSum).
For each ngram key:
Emit the global count along with the trigram data for further processing:
emit(key, lastSum:triplet1)
emit(key, lastSum:triplet2)
...

-----------------------------------------------------------

Step 2: Calculating Probabilities

Mapper:
For each pair (key, count:triplet_i):
Analyze the key to determine if it represents one of the following parameters: N1, N2, N3, C0, C1, or C2.
Emit the corresponding result:
emit(triplet_i, "N_j=" + count) (where N_j or C_j is the identified parameter).


Reducer:
Initialize all parameters (N1, N2, ..., C0, C1, C2) to -1.
For each pair (triplet, "N_j=" num):
Update the corresponding parameter value.
Once all required values are initialized, calculate the probability.
Emit the result:
emit(triplet, probability)


-----------------------------------------------------------

Step 3: Sorting the Output
This step handles sorting the results and trigrams as specified in the assignment. No additional processing logic is required here.