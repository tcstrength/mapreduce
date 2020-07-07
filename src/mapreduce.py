from multiprocessing import Pool
import collections
import itertools

class MapReduce:

    def __init__(self, mapper, reducer, num_workers = None):
        self.pool = Pool(num_workers)
        self.reducer = reducer
        self.mapper = mapper

    def partition(self, mapped_values):
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    def __call__(self, inputs, chunksize=2):
        map_responses = self.pool.map(self.mapper, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        print(partitioned_data)
        reduced_values = self.pool.map(self.reducer, partitioned_data)
        return reduced_values


