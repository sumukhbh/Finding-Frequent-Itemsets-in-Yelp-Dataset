from pyspark import SparkContext
import sys
import time
from collections import defaultdict
sc = SparkContext("local[*]", "Sumukh_Task2")
sc.setLogLevel("OFF")
start = time.time()
filter_threshold = int(sys.argv[1])
support = int(sys.argv[2])
input_file_path = sys.argv[3]
RDD_inter = sc.textFile(input_file_path)
partition = RDD_inter.getNumPartitions()
#saving the header
header = RDD_inter.first()
result = RDD_inter.filter(lambda x: x != header)
#print(result.take(5))
data = result.map(lambda a: a.split(","))
new_data = data.map(lambda b: (b[0],[b[1]]))
basket_data = new_data.reduceByKey(lambda a, b: a+b)

filter_data = basket_data.filter(lambda a: len(a[1]) > filter_threshold).map(lambda a: (a[0],a[1]))
#print(len(filter_data))

# Filter the Keys #
basket_rdd = filter_data.map(lambda a: (a[1]))
total_baskets = basket_rdd.count()
#print(basket_rdd.take(5))

# Apriori Algorithm #
def a_priori(basket_data,support,total_baskets):
    data = list(basket_data)
    threshold = support * float(len(data)/total_baskets)
    candidate_items = []
    candidate_pairs = {}
    candidate_sets = set()
    for basket in data:
         for item in basket:
             if item in candidate_pairs:
                 candidate_pairs[item] += 1
             else:
                 candidate_pairs[item] = 1
    for single in sorted(candidate_pairs.keys()):
        if candidate_pairs[single] >= threshold:
            if single not in candidate_items:
                candidate_items.append((single,1))
                candidate_sets.add(frozenset([single]))
    #freq_count = candidate_pairs.keys()
    #print(candidate_sets)
    set_size = 2
    while True:
        candidate_sets = set([item1.union(item2) for item1 in candidate_sets for item2 in candidate_sets if
                             len(item1.union(item2)) == set_size])
        #print(list(generate_candidates))
        candidate_counts = defaultdict(lambda: 0)
        candidate_set = set()
        for basket in data:
            bas = set(basket)
            #print("Basket is {}".format(basket))
            for candidate in candidate_sets:
                #print("Candidate is: {}".format(candidate))
                #print("Set of candidate {} and Set of basket is {}".format(set(candidate), set(basket)))
                if candidate.issubset(bas):
                   if candidate in candidate_counts:
                       candidate_counts[candidate] += 1
                   else:
                       candidate_counts[candidate] = 1
                if candidate_counts[candidate] >= threshold:
                    if (candidate,1) not in candidate_items:
                        candidate_items.append((candidate,1))
                        candidate_set.add(candidate)

        if len(candidate_set) == 0:
            break
        candidate_sets = candidate_set
        set_size = set_size + 1
#     #print(candidate_set)
#     #print(candidate_items)
    return candidate_items

Mapper_1 = basket_rdd.mapPartitions(lambda basket_data: a_priori(basket_data,support,total_baskets))
Reduce_inter = Mapper_1.reduceByKey(lambda a, b: 1).map(lambda a: a[0]).collect()
converter = []
for b in Reduce_inter:
        if type(b) != str:
            converter.append(tuple(sorted(b)))
        else:
            converter.append(tuple([b]))

converter = sorted(converter, key=lambda b: (len(b), b))
opened_file = open(sys.argv[4], 'a')
opened_file.write("Candidates:\n")
opened_file.close()
item_size = 1
output_string = ""
for item in converter:
    new_item = str(item)
    if len(item) == 1:
        new_item = new_item.replace(",", "")
        output_string = output_string + new_item + ","
    elif len(item) == item_size:
        output_string = output_string + new_item + ","
    else:
        output_string = output_string[:-1] + "\n\n"
        final_item = str(item)
        output_string = output_string + final_item + ","
        item_size = len(item)
opened_file = open(sys.argv[4], 'a')
opened_file.write(str(output_string[:-1]))
opened_file.close()
# print(len(Reduce_inter))

# opened_file = open("task.txt", "r")
# count = 0
# for line in opened_file:
#     count += opened_file.readline().count(')')

#print("total tokens ",count)
#print(Reducer_1.collect())
Reducer_1 = Mapper_1.reduceByKey(lambda a, b: 1).collect()
#print(len(Reducer_1))
def final_phase(basket_data):
     data = list(basket_data)
     final_count = {}
     for count in Reducer_1:
          final_count[count[0]] = 0
     for item in data:
         item = set(item)
         for item_set in final_count.keys():
             if item_set in item:
                  final_count[item_set] += 1
             else:
                  if set(item_set).issubset(item):
                    final_count[item_set] += 1
     return final_count.items()

Mapper_2 = basket_rdd.mapPartitions(final_phase)
Reducer_2 = Mapper_2.reduceByKey(lambda a, b: a + b).filter(lambda a: a[1] >= support).map(lambda a: a[0]).collect()
converter = []
for b in Reducer_2:
        if type(b) != str:
            converter.append(tuple(sorted(b)))
        else:
            converter.append(tuple([b]))

converter = sorted(converter, key=lambda b: (len(b), b))
opened_file = open(sys.argv[4], 'a')
opened_file.write("\n")
opened_file.write("\nFrequent Itemsets:\n")
opened_file.close()
item_size = 1
output_string = ""
for item in converter:
    new_item = str(item)
    if len(item) == 1:
        new_item = new_item.replace(",", "")
        output_string = output_string + new_item + ","
    elif len(item) == item_size:
        output_string = output_string + new_item + ","
    else:
        output_string = output_string[:-1] + "\n\n"
        final_item = str(item)
        output_string = output_string + final_item + ","
        item_size = len(item)
opened_file = open(sys.argv[4], 'a')
opened_file.write(str(output_string[:-1]))
opened_file.close()
#print(result)
#print(len(Reducer_2))
end = time.time()
print("Duration:", end - start)







































