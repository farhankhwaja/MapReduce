__author__ = 'farhankhwaja'
import MapReduce
import sys

"""
    Mutual Friends Program in the Python 3.4.2 MapReduce Framework
    """

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key:  (PersonA + Friend of PersonA) or (Friend of PersonA + PersonA)
    # value: Friends of PersonA
    key = record[0]
    value = record[1]
    for friend in value:
        # Checking whether PersonA or Friend of PersonA is greater. Then emitting the combination as key
        if friend > key:
            mr.emit_intermediate((key + friend), value)
        else:
            mr.emit_intermediate((friend + key), value)


def reducer(key, list_of_values):
    # key:  (PersonA + Friend of PersonA) or (Friend of PersonA + PersonA)
    # value: Mutual Friend
    mutual_friend = []

    """ Checking only for those key where list_of_values has more than one list in it. Thus reducing the no. of checks
    """
    if len(list_of_values) >= 2:
        for i in range(len(list_of_values) - 1):
            for friend in list_of_values[i]:
                if friend in list_of_values[i + 1]:
                    mutual_friend.append(friend)
        if mutual_friend:
            mr.emit((key, mutual_friend))
    mr.result.sort()

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
