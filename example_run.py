from enhanced_handler import handle_question

# Example user queries
questions = [
    "Compare average rainfall between Karnataka and Maharashtra",
    "Highest and lowest production for Wheat in Karnataka and Maharashtra",
    "Policy advise between Wheat and Rice in Punjab",
    "Trend of Wheat in Punjab"
]

for q in questions:
    print(f"\nQuery: {q}")
    res = handle_question(q)
    print("Result:", res)
