__author__ = 'farhankhwaja'
import MapReduce
import sys

"""
    Pair of friends Program in Python 3.4.2 MapReduce Framework
    """

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key:  PersonA
    # value: Friends of PersonA
    key = record[0]
    value = record[1]
    for friend in value:
        if not mr.intermediate:
            mr.emit_intermediate(1,(key, friend))
        else:
            mr.emit_intermediate(max(mr.intermediate.keys())+1, (key, friend))


def reducer(key, list_of_values):
    # key: Unique Number
    # value: Pair of Friends
    for v in list_of_values:
        if ((v[1],v[0]) in [x for v in mr.intermediate.values() for x in v]) and ((v[1], v[0]) not in mr.result and (v[0], v[1]) not in mr.result):
            if v[1] > v[0]:
                mr.emit((v[0], v[1]))
            else:
                mr.emit((v[1], v[0]))
    mr.result.sort()

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
