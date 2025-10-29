import json
from flask import Flask, render_template, request
from enhanced_handler import handle_question

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html', answer=None, sources=[])

@app.route('/ask', methods=['POST'])
def ask():
    question = request.form['question']
    try:
        response = handle_question(question)

        # Format response nicely
        answer_text = json.dumps(response['answer'], indent=4) if 'answer' in response else str(response)

        return render_template(
            'index.html',
            answer=answer_text,
            sources=response.get('sources', [])
        )
    except Exception as e:
        return render_template('index.html', answer=f"Error: {str(e)}", sources=[])

if __name__ == '__main__':
    app.run(debug=True)
