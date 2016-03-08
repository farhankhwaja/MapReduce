__author__ = 'farhankhwaja'
import MapReduce
import sys
import operator

"""
    Friend List Program in Python 3.4.2 MapReduce Framework
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
