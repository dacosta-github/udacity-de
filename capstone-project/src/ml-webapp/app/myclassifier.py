import streamlit as st 
import joblib,os
import sys
sys.path.insert(0, '../src')
sys.path.insert(0, '../src/ml-webapp/')
sys.path.insert(0, '../src/ml-webapp/models')
sys.path.insert(0, '../models')

# load Vectorizer for complaints narrative
vectorizer = open("../models/word_vectorizer.pkl","rb")
tfidf_v = joblib.load(vectorizer)

# load prodict model
def load_prediction_models(model_file):
	loaded_model = joblib.load(open(os.path.join(model_file),"rb"))
	return loaded_model

def main():
	"""Classifier"""

	html_temp = """
    <div style="background-color:green;padding:10px">
    <h1 style="color:white;text-align:center;">Complaints Text Classification</h1>
    </div>
	"""

	st.markdown(html_temp,unsafe_allow_html=True)

	choice = 'Prediction'

	if choice == 'Prediction':
		st.info("Prediction with Machine Learning Model (ML)")

		complaint_text = st.text_area("Enter complaint text narrative here "," ")
		all_ml_models = ["Logistic Regression","Linear SVC"]
		model_choice = st.selectbox("Select ML model",all_ml_models)

		if st.button("Classify"):
			st.text("Original text: \n{}".format(complaint_text))
			vect_text = tfidf_v.transform([complaint_text]).toarray()
			if model_choice == 'Logistic Regression':
				predictor = load_prediction_models("../models/LogisticRegression.pkl")
				prediction = predictor.predict(vect_text)
				
			elif model_choice == 'Linear SVC':
				predictor = load_prediction_models("../models/LinearSVC.pkl")
				prediction = predictor.predict(vect_text)

			final_result = prediction
			st.success("Complaint classified with the product: {}".format(final_result))

if __name__ == '__main__':
	main()