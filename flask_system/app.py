from flask import Flask
from pymilvus import connections
app = Flask(__name__)

# host = '127.0.0.1'
# port = '19530'
# connections.add_connection(default={"host": host, "port": port})
# connections.connect(alias='default')

# from pymilvus import Collection, DataType, FieldSchema, CollectionSchema
# dim = 128
# id_field = FieldSchema(name="id", dtype=DataType.INT64, description="primary_field")
# year_field = FieldSchema(name="year", dtype=DataType.INT64, description="year")
# embedding_field = FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim)
# schema = CollectionSchema(fields=[id_field, year_field, embedding_field], primary_field='id', auto_id=True, description='desc of collection')
# collection_name = "tutorial"
# collection = Collection(name=collection_name, schema=schema)

@app.route('/')
def hello_world():
    # return 'Hello Flask!'
    return "<p>Hello, World!</p>"

# @app.route('/search')
# def search_for_vector():


if __name__ == '__main__':
    app.run()