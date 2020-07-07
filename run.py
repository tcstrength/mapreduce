from src.mapreduce import MapReduce
from src.chunk import chunkify
import multiprocessing
import string

def read_file(filename):
    output = []

    with open(filename, 'rt') as f:
        return f.read().lower().split()

    return output

def mapper(input):
    output = []

    for word in input:
        output.append((word, 1))

    return output

def reducer(item):
    word, occurances = item
    return (word, sum(occurances))

data = read_file('data/test1.txt')
chunks = chunkify(data, 32)
mapreduce = MapReduce(mapper, reducer)
print(mapreduce(chunks))