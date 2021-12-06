from datetime import datetime
from flask import Flask, send_file, jsonify
from flask import request
from flask_cors import CORS
from random import randint
import os

from kafka_py.milvus_model import MilvusModel

app = Flask(__name__)
CORS(app)

# milvus
# milvus_model = getMilvus()

# sql Model
# sql_M = SQLModel(Mode=2) # query mode

@app.route('/')
def hello_world():
    # return 'Hello Flask!'
    return "This is Flask"

@app.route("/hashtag_rank", methods=["GET"])
def hashtag_rank():
    file_name = "WordCloud.png"
    png_path = os.getcwd() + "/" + file_name
    response = jsonify({"png_path": png_path, "file_name": file_name})
    return response
import json
@app.route("/insert", methods = ['POST', 'GET'])
def insert():
    data = request.form.get("data")
    data = json.loads(data)
    vectors = data["vector"]
    ids = data['id']
    print(vectors, type(vectors))
    response = jsonify({"msg_code": 500})
    
    return response
    # milvus_model.insert(data)

@app.route("/search")
def search():
    content = request.args.get("content")
    # import XXX
    # XXX()
    # response = XXX
    id_results = MilvusModel.search()
    text_results = sql_M.query(id_results)
    message_0 = {"time": datetime.now(), "content": content, "hashtag": "#" + str(randint(0, 100))}
    message_1 = {"time": "2021-12-01", "content": "This is the First Tweet.", "hashtag": "#Hello"}
    message_2 = {"time": "2021-12-02", "content": "This is the Second Tweet.", "hashtag": "#Lakers"}
    message_3 = {"time": "2021-12-03", "content": "This is the Third Tweet.", "hashtag": "#Warriors"}
    message_list = [message_0, message_1, message_2, message_3]
    response = jsonify({"message_list": message_list})
    return response

if __name__ == '__main__':
    app.run(debug=True)
