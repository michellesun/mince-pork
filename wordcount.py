import mincemeat
from stopwords import allStopWords
import glob

# to compute how many times every term occurs across titles, for each author.
# Alberto Pettorossi the following terms occur in titles with the indicated cumulative frequencies (across all his papers): program:3, transformation:2, transforming:2, using:2, programs:2, and logic:2.

text_files = glob.glob('hw3data/*')

def findtitleauthor(text_files):
    list_of_tuples = []
    for file_name in text_files:
        f = open(file_name)
        for line in f:
            line = line.split(':::')
            title = line[-1]
            author = line[1].split('::')
            pair = (title,author)
            list_of_tuples.append(pair)
    return list_of_tuples # [('Applications of temporal Databases to 
    # Knowledge-based Simulations.', ['Alexander Tuzhilin']), 
    #('Dynamic Binding in Mobile Applications: A Middleware Approach.',
    # ['Paolo Bellavista', 'Antonio Corradi', 'Rebecca Montanari', 'Cesare Stefanelli']) ]



def makesource(lot): #lot is short for list of tuples
    for pair in lot:
        # ('Dynamic Binding in Mobile Applications: A Middleware Approach.', ['Paolo Bellavista', 'Antonio Corradi', 'Rebecca Montanari', 'Cesare Stefanelli'])
        source = dict(enumerate(pair[1]))
        i = 0
        for i in range(len(lot)):
            lot[i] = (lot[i],pair[0]) 
    return source
    #  {1: ('Antonio Corradi', 'Dynamic Binding in Mobile Applications: A Middleware Approach.'), 2: ('Rebecca Montanari', 'Dynamic Binding in Mobile Applications: A Middleware Approach.'), 3: ('Cesare Stefanelli', 'Dynamic Binding in Mobile Applications: A Middleware Approach.')}

# def file_contents(file_name):
#     f = open(file_name)
#     try:
#         return f.read()
#     finally:
#         f.close()

# source = dict((file_name, file_contents(file_name)) for file_name in text_files)

# f = open('outfile','w')
# def final(key, value):
#     print key, value
#     f.write(str(key,value))

### mincemeat py example program 
# data = ["Humpty Dumpty sat on a wall",
#         "Humpty Dumpty had a great fall",
#         "All the King's horses and all the King's men",
#         "Couldn't put Humpty together again",
#         ]

#  {0: ('Paolo Bellavista', 'Dynamic Binding in Mobile Applications: A Middleware Approach.'), 1: ('Antonio Corradi', 'Dynamic Binding in Mobile Applications: A Middleware Approach.'), 2: ('Rebecca Montanari', 'Dynamic Binding in Mobile Applications: A Middleware Approach.'), 3: ('Cesare Stefanelli', 'Dynamic Binding in Mobile Applications: A Middleware Approach.')}

def mapfn(k, v): #split the title
    for w in v[1].split():
        first = (v[0], w)
        yield first, 1

def reducefn(k, vs):
    result = 0
    for v in vs:
        result += v
    return result

s = mincemeat.Server()

# The data source can be any dictionary-like object
list_of_tuples = findtitleauthor(text_files)
s.datasource = makesource(list_of_tuples)
# s.datasource = dict(enumerate(data))
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")
print results