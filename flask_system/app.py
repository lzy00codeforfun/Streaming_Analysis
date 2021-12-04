from flask import Flask, send_file, jsonify
from flask_cors import CORS
import os
app = Flask(__name__)
CORS(app)

@app.route('/')
def hello_world():
    # return 'Hello Flask!'
    return "<p>Hello, World!</p>"

# @app.route('/search')
# def search_for_vector():

@app.route("/hashtag_rank", methods=["GET"])
def hashtag_rank():
    png_path = os.getcwd() + "/word_cloud.png"
    response = jsonify({"png_path": png_path})
    print("???" * 10)
    return response
    # return send_file("word_cloud.png")


if __name__ == '__main__':
    app.run(debug=True)