# MapReduce
This exercise helped me in understanding MapReduce paradigm. As I designed and implemented MapReduce algorithms for a variety of common data processing tasks.
<br />
<br />
It has 5 different Python files, with each performing its own task.<br />
1. <b>size_count.py</b> : A python program, that implements a mapReduce algorithm to count the words of each size (large, medium, small, tiny) in a document.<br />
2. <b>list_friends.py</b> : Given a simple social network dataset consisting of a set of key-value pairs (person, friend) representing a friend relationship between two people. This python program implements a MapReduce algorithm to produce a complete list of friends for each person.<br />
3. <b>pairs_of_friends.py</b> : A python program implements a MapReduce algorithm to identify symmetric friendships in the input data. The program will output pairs of friends where personA is a friend of personB and personB is also a friend of personA. If the friendship is asymmetric (only one person in the pair considers the other person a friend), do not emit any output for that pair.<br/>
4. <b>mutual_friends.py</b> : A python program implements a MapReduce algorithm to identify mutual friends for a pair of friends. The program will output a list of friends that personA and personB are both friends with.<br/>
5. <b>relational_join.py</b> : Implement a MapReduce algorithm to join the two relations based on the Movie ID/Rated Movie ID value. A file with two relations that represent user information names and user movie ratings, is provided. 
