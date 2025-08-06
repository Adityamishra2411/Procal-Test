import requests
import os

def read_file_content(file_path):
    if not os.path.exists(file_path):
        return None, f"File not found: {file_path}"
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content, None
    except Exception as e:
        return None, f"Error reading file: {e}"

def send_to_webhook(file_content):
    url = "http://localhost:5678/webhook-test/data-pipeline"
    payload = {
        "file_data": file_content,
        "source": "chatbot"
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json().get("reply", "No reply field in response.")
    except requests.exceptions.RequestException as e:
        return f"Error: {e}"

def chatbot_with_file():
    print("📂 Chatbot with file input. Type 'exit' to quit.")
    while True:
        file_path = input("Enter file path: ")
        if file_path.lower() == "exit":
            print("Chatbot: Goodbye!")
            break
        
        content, error = read_file_content(file_path)
        if error:
            print(f"❌ {error}")
            continue
        
        reply = send_to_webhook(content)
        print(f"Chatbot: {reply}")

if __name__ == "__main__":
    chatbot_with_file()