import requests

questions = [
    "What is Azure Functions?",
    "Explain Event Hub.",
    "What is a Timer Trigger?"
    # "How does Blob storage work?",
    # "What is a Queue Trigger?"
]

for q in questions:
    res = requests.post("http://localhost:7071/api/enqueue", json={"question": q})
    print(q, "->", res.status_code, res.text)
