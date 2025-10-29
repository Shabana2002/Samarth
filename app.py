import json
from flask import Flask, render_template, request
from enhanced_handler import handle_question  # Updated import

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/ask', methods=['POST'])
def ask():
    question = request.form['question']
    try:
        response = handle_question(question)  # Use the new handler

        # Convert dicts nicely for HTML
        if isinstance(response, dict):
            answer_text = json.dumps(response, indent=4)
        else:
            answer_text = str(response)

        return render_template(
            'index.html',
            answer=answer_text,
            sources=response.get('sources', []) if isinstance(response, dict) else []
        )
    except Exception as e:
        return render_template('index.html', answer=f"Error: {str(e)}", sources=[])

if __name__ == '__main__':
    app.run(debug=True)
