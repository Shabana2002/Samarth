from flask import Flask, request, render_template, jsonify
from enhanced_handler import compare_states_average_rainfall, get_top_crops, analyze_crop_trend, policy_advice

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/ask_ui", methods=["POST"])
def ask_ui():
    data = request.json
    qtype = data.get("type")
    if qtype == "rainfall_compare":
        answer = compare_states_average_rainfall(data.get("stateX"), data.get("stateY"))
    elif qtype == "top_crops":
        answer = get_top_crops(data.get("stateX"), data.get("stateY"), top_m=data.get("topN", 3))
    elif qtype == "crop_trend":
        answer = analyze_crop_trend(data.get("crop"), data.get("region"))
    elif qtype == "policy_advice":
        answer = policy_advice(data.get("cropA"), data.get("cropB"), data.get("state"))
    else:
        answer = {"answer": "Invalid question type.", "sources": []}
    return jsonify(answer)

if __name__ == "__main__":
    app.run(debug=True)
