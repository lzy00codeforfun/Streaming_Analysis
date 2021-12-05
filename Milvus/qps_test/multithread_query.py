# -*- coding: utf-8 -*-
#Connectings to Milvus and Redis

# import redis
from pymilvus import *
from concurrent.futures import ThreadPoolExecutor

thread_num = 2
query_num = 1000
_pool = ThreadPoolExecutor(max_workers=thread_num)
connections.connect(host="127.0.0.1", port=19530)

def search(query_embeddings, query_ids):
    # global connections
    st = time.time()
    query_nums = len(query_ids)
    field_name = "text_vector"
    results = collection.search(query_embeddings, field_name, param=search_params, limit=9, expr=None)
    print('query_num: {}, query_id: {} to {}, search_time: {}'\
            .format(query_nums, query_ids[0], query_ids[-1], time.time()-st))
    return True


search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
import time
import numpy as np

collection = Collection("test_vector_search")
collection.load()

steps = thread_num
query_ids = [i for i in range(query_num )]
query_ids = np.array(query_ids).reshape(steps, query_num//steps).tolist()
query_embeddings = np.random.rand(query_num, 1, 400)\
                        .reshape(steps, query_num//steps, 400)\
                        .tolist()
tot_st = time.time()
results = _pool.map(search, query_embeddings, query_ids)
# print("results", np.array(results).sum())
for i in results:
    continue
    # print(type(i), "!!!")
print('total time: {}, query_num: {}'.format(time.time()-tot_st, query_num))

# while True:
    # st = time.time()
    # collection = Collection("test_vector_search")
    # collection.load()
    # print(">>>>>>>>>>>>>>>")
    # print('load time', time.time()-st)
    # print('collection.num_entities', collection.num_entities)

#     st = time.time()
#     query_embeddings = np.random.rand(1, 400).tolist()



#     print('\n\n')
    # print(results)
    # collection.release()
#     time.sleep(3)

