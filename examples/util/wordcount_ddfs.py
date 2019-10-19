from disco.worker.task_io import chain_reader
from disco.core import Job, result_iterator
from disco.worker.classic.func import sum_reduce

def map(line, params):
    for word in line.split("\n"):
        yield word, 1


if __name__ == '__main__':
    job = Job().run(input=["tag://data:clusters"],
                    map_reader=chain_reader,
                    map=map,
                    partitions=9,
                    sort=None,
                    reduce=sum_reduce,  # reduce
                    save_results=True)
    # save results to DDFS
    job.wait(show=True)
