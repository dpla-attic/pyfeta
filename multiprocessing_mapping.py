from multiprocessing import *
from os import scandir
from os.path import isfile, join
from cdl_map import CdlMap
import sys


def handle_file(in_path, out_path, filename):
    with open(join(in_path, filename), 'r') as in_file:
        output = CdlMap.map(in_file)
        with open(join(out_path, filename), 'w') as out_file:
            out_file.write(output.decode('utf8'))
        return None

def handle_error(e):
    """Exception handler for multiprocessing.Pool.apply_async"""
    # would log the exception here and increment a counter
    print("caught exception: ", e)

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: multiprocessing_mapping <IN_DIR> <OUT_DIR>",
              file=sys.stderr)
        exit(-1)

    fcount = 0
    with Pool(processes=4) as pool:
        for f in scandir(sys.argv[1]):
            fcount += 1
            if f.is_file():
                pool.apply_async(handle_file,
                                 args=(sys.argv[1], sys.argv[2], f.name),
                                 error_callback=handle_error)
        pool.close()
        pool.join()

    print("Considered %d files" % fcount)
