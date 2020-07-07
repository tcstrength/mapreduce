from itertools import islice

def chunkify(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())