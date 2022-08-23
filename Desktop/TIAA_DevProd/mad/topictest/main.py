import os
# os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

import io
from flask import Flask, request, jsonify
import top2vec_inference

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        try:
            result = top2vec_inference.prediction()
            return jsonify({"success": result})
        except Exception as e:
            return jsonify({"error": str(e)})

    return "OK"

if __name__ == "__main__":
    app.run(debug=True)