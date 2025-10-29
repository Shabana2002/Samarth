import json
from flask import Flask, render_template, request
from enhanced_handler import handle_question  # Your existing handler

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/ask', methods=['POST'])
def ask():
    question = request.form['question']
    try:
        response = handle_question(question)  # Use your handler

        # Convert dict to nicely formatted JSON for display
        if isinstance(response, dict):
            answer_text = json.dumps(response, indent=4)
            sources = response.get('sources', [])
        else:
            answer_text = str(response)
            sources = []

        return render_template(
            'index.html',
            answer=answer_text,
            sources=sources
        )
    except Exception as e:
        return render_template('index.html', answer=f"Error: {str(e)}", sources=[])

if __name__ == '__main__':
    app.run(debug=True)
