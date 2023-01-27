
main={
    "ip":"127.0.0.1",
    "port":4444,
    "input":['./input/1200.txt','./input/ulyss11.txt','./input/17502.txt','./input/11757.txt'],
    "output":'./output/test.txt',
    "mappers":4,
    "reducers":4,
    # "application":"word_count",
    "application":"inverted_index",
    "fail_random_mappers":True,
    "dummy":"not in use"
}