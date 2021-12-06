# -*- coding: utf-8 -*-
#Connectings to Milvus
from pymilvus import *
import random
import numpy as np

connections.connect(host="127.0.0.1", port=19530)

class MilvusModel(object):
    def __init__(self, collection_name="test_vector_search"):
        super().__init__()
        self.collection_name = collection_name
        self.schema = CollectionSchema([
                    FieldSchema("text_id", DataType.INT64, is_primary=True),
                    FieldSchema("text_vector", dtype=DataType.FLOAT_VECTOR, dim=400)
            ])
        
        self.collection = Collection(self.collection_name, self.schema, \
                                        using='default', shards_num=5)

    
    def drop():
        collection = Collection(self.collection_name)
        collection.drop()
    
    def random_insert(self, data_num=1000, batch_size=2000):
        cur_size = 0
        while cur_size <= data_num:
            data = [
                        [i for i in range(0, cur_size)],
                        np.random.rand(cur_size, 400).tolist()
                    ]
            cur_size += batch_size
            print(type(data), type(data[0]), type(data[0][0]))
            self.collection.insert(data)
            index_param = {
                            "metric_type":"L2",
                            "index_type":"IVF_SQ8",
                            "params":{"nlist":1024}
                    }
            self.collection.create_index("text_vector", index_params=index_param)

    def insert(self, insert_data, batch_size):
        cur_size = 0
        data_num = len(insert_data['id'])
        while cur_size <= data_num:
            data = [
                        insert_data['id'][cur_size: cur_size + batch_size],
                        insert_data['vector'][cur_size: cur_size + batch_size],
                    ]
            cur_size += batch_size
            print(type(data), type(data[0]), type(data[0][0]))
            self.collection.insert(data)
            index_param = {
                            "metric_type":"L2",
                            "index_type":"IVF_SQ8",
                            "params":{"nlist":1024}
                    }
            self.collection.create_index("text_vector", index_params=index_param)

    def search(self, query_embeddings):
        # self.collection.load()
        field_name = "text_vector"
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        results = self.collection.search(query_embeddings, field_name, param=search_params, limit=9, expr=None)
        return results

