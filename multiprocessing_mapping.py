from multiprocessing import *
from os import listdir
from os.path import isfile, join
from cdl_map import CdlMap
import sys


def handle_file(in_path, out_path, file):
    # print(file)
    with open(join(in_path, file), 'r') as in_file:
        try:
            output = CdlMap.map(in_file)
            with open(join(out_path, file), 'w') as out_file:
                out_file.write(output.decode('utf8'))

            return "SUCCESS: " + file

        except:
            with open(join(out_path, file), 'w') as out_file:
                out_file.write(str(sys.exc_info()[0]))

            return "FAILURE: " + file

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: multiprocessing_mapping <IN_DIR> <OUT_DIR>", file=sys.stderr)
        exit(-1)

    pool = Pool(processes=4)
    files = [pool.apply_async(handle_file, (sys.argv[1], sys.argv[2], f)) for f in listdir(sys.argv[1]) if isfile(join(sys.argv[1], f))]
    for res in files:
        print(res.get(timeout=1))


