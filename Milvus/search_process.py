# -*- coding: utf-8 -*-
#Connectings to Milvus and Redis

# import redis
from pymilvus import *

connections.connect(host="127.0.0.1", port=19530)



search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
import time
import numpy as np
while True:
    st = time.time()
    collection = Collection("test_vector_search")
    collection.load()
    print(">>>>>>>>>>>>>>>")
    print('load time', time.time()-st)
    print('collection.num_entities', collection.num_entities)

    st = time.time()
    query_embeddings = np.random.rand(1, 400).tolist()
    field_name = "text_vector"
    results = collection.search(query_embeddings, field_name, param=search_params, limit=9, expr=None)
    print('search time', time.time()-st)
    print('\n\n')
    # print(results)
    # collection.release()
    time.sleep(3)

