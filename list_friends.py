__author__ = 'farhankhwaja'

import MapReduce
import sys
import operator

"""
Given a simple social network dataset consisting of a set of key-value pairs (person, friend)
representing a friend relationship between two people. This python program implements a MapReduce
algorithm to produce a complete list of friends for each person.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: PersonA
    # value: PersonB
    key = record[0]
    value = record[1]
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: document identifier
    # value: list of friends
    mr.emit((key, list_of_values))
    mr.result.sort()

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
