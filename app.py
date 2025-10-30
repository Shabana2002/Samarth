from flask import Flask, request, jsonify, render_template
from matching import answer_question

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/ask', methods=['POST'])
def ask():
    data = request.get_json()
    if not data or 'question' not in data:
        return jsonify({"error": "No question provided"}), 400

    question_text = data['question']
    answer = answer_question(question_text)
    return jsonify(answer)

if __name__ == '__main__':
    app.run(debug=True)
