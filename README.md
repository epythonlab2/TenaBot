# TenaBot: AI-Powered Personalized Medication Advisor

## Overview
TenaBot is a smart AI assistant that helps patients understand and follow medication instructions. It focuses on the Ethiopian context, translating drug information into local languages and providing guidance in a simple, user-friendly format.

## Key Features
- Generates easy-to-understand medication instructions.
- Supports multiple Ethiopian languages.
- Offers dosage guidance, warnings, and side-effect info.
- Conversational chatbot interface for patient queries.
- Optional voice guidance with text-to-speech.

## Installation Instructions
1. Clone the repository:
```bash
git clone https://github.com/epythonlab2/TenaBot.git
cd TenaBot
```

2. Set up a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Data Pipeline
![alt text](screenshot/image.png)
![alt text](screenshot/image-1.png)

## Usage
### Start API Backend
```bash
cd src/api
uvicorn app:app --reload
```
Access endpoints at `http://127.0.0.1:8000`.

### Run Frontend (Optional)
If using Streamlit:
```bash
cd app
streamlit run app.py
```

## Contributing
1. Fork the repository.
2. Create a branch: `git checkout -b feature/YourFeature`
3. Commit changes: `git commit -m 'Add feature'
4. Push branch: `git push origin feature/YourFeature`
5. Open a pull request.

## License
MIT License

## Contact
Email: asibeh.tenager@gmail.com
