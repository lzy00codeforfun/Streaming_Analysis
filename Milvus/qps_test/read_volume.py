# -*- coding: utf-8 -*-
#Connectings to Milvus and Redis

# import redis
from pymilvus import *

connections.connect(host="127.0.0.1", port=19530)


collection = Collection("test_vector_search")
collection.load()
print('collection.num_entities', collection.num_entities)

collection.release()
