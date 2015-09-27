#!/usr/bin/python

import os, sys
import pprint

env_prefix = 'RUNONHADOOP_'

def get_input_files(env):
    files = []
    input_count = int(env.get(env_prefix + 'HDFS_INPUT_COUNT', 0))
    for i in xrange(input_count):
        fname = env.get(env_prefix + "HDFS_INPUT_%d" % i)
        if fname:
            files.append(fname)
    return files



def main():
    input_files = get_input_files(os.environ)

    temp_dir = os.environ.get(env_prefix + 'TEMP_DIR')
    hdfs_output_dir = os.environ.get(env_prefix + 'HDFS_OUTPUT_DIR')
    hdfs_itermediate_output_dir = os.environ.get(env_prefix + 'HDFS_INTERMEDIATE_DIR')
    task_uid = os.environ.get(env_prefix + 'UID')

    local_fname = "tempfile.txt"

    print "cwd:" , os.getcwd()

    print "os.environ"
    pprint.pprint(os.environ)

    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_PREFIX"], "bin")


    open(local_fname, "wt").write("\n".join(input_files))
    os.system("hadoop fs -put %s %s/testout-%s.txt" % (local_fname, hdfs_output_dir, task_uid))

    open(local_fname, "wt").write("intermediate: \n" + "\n".join(input_files))
    os.system("hadoop fs -put %s %s/testintout-%s.txt" % (local_fname, hdfs_itermediate_output_dir, task_uid))

    #~ os.unlink(local_fname)






    return 0

if __name__ == '__main__':
    main()
