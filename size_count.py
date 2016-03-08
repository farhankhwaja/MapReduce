__author__ = 'farhankhwaja'
import MapReduce
import sys
from collections import OrderedDict

"""
    Size Count Program in Python 3.4.2 MapReduce Framework
    """

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    large = 0
    medium = 0
    small = 0
    tiny = 0
    for w in words:
        if len(w) >= 10:
            large += 1
        elif 5 <= len(w) <= 9:
            medium += 1
        elif 2 <= len(w) <= 4:
            small += 1
        else:
            tiny += 1

    mr.emit_intermediate(key,["large",large])
    mr.emit_intermediate(key,["medium",medium])
    mr.emit_intermediate(key,["small",small])
    mr.emit_intermediate(key,["tiny",tiny])

def reducer(key, list_of_values):
    # key: document identifier
    # value: list of large, medium, small & tiny word count
    mr.emit((key, list_of_values))

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
