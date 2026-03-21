install:
	pip install -r requirements.txt

run-api:
	uvicorn api.main:app --reload

run-ui:
	streamlit run app/streamlit_app.py