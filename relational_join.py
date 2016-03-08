__author__ = 'FarhanKhwaja'

import MapReduce
import sys

"""
Implement a MapReduce algorithm to join the two relations based on the Movie ID/Rated Movie ID value. 
A file with two relations that represent user information names and user movie ratings, is provided.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: MovieID
    # value: (Relation,Values)
    relation = record[0]
    value = record[1:]
    if relation == "MovieNames":
        mr.emit_intermediate(value[1], (relation, value[0]))
    else:
        mr.emit_intermediate(value[0], (relation, value[1:]))

def reducer(key, list_of_values):
    # key: MoveID
    # value: (Movie Name, MovieID, Rated Movie ID, User ID, User Rating) & (MovieNam,Average Rating)

    list_rating = []
    rating_sum = 0
    movie_name = ""

    # Creating Rating List
    for Values in list_of_values:
            if Values[0] == "MovieNames":
                movie_name = Values[1]
            else:
                list_rating.append(Values)

    for ratings in list_rating:
        for rate in ratings[1:]:
            # emit(Movie Name, MovieID, Rated Movie ID, User ID, User Rating)
            mr.emit((movie_name, key, key, rate[0], rate[1]))
            # summation of all user ratings
            rating_sum += rate[1]

    # emit(MovieName,AvgRating)
    mr.emit((movie_name, rating_sum/len(list_rating)))


# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
