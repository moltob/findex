import pathlib

from fhash import index_directory, count_files


def main():
    # findex = Index(pathlib.Path('test.db'))
    # findex.create()

    TEST_DIR = pathlib.Path(r'Q:\Britta Pagel Fotografie')

    result = None

    def timed_main():
        nonlocal result
        result = list(index_directory(TEST_DIR))

    import timeit

    count_time = timeit.timeit(lambda: count_files(TEST_DIR), number=1)
    exec_time = timeit.timeit(timed_main, number=1)

    numbytes = 0
    for fdesc in result:
        print(fdesc.fhash, fdesc.path)
        numbytes += fdesc.size

    kbits = 8 / 1024 * numbytes / exec_time
    print(f'Count time:     {count_time:.3f} s')
    print(f'Execution time: {exec_time:.3f} s')
    print(f'Files:          {len(result)} ({numbytes:,d} bytes)')
    print(f'Bandwith:       {kbits:.2f} kbit/s')


if __name__ == '__main__':
    main()
