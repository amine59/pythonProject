import streamlit as st
import pandas as pd
import seaborn as sns
import os
import matplotlib.pyplot as plt
import numpy as np
import plotly.graph_objects as go
from plotly import tools
import plotly.offline as py
import plotly.express as px
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
import sys
from pandas.errors import ParserError
import time
import statsmodels.api as sm

st.title('Machine Learning AMARIS DEMO')

class application_stlit:
    def prepare_data(self, lit_dataframe):
        self.feat_y = self.features[0]
        self.y = lit_dataframe[[self.feat_y]]
        self.feat_x = lit_dataframe[self.features]
        self.x = self.feat_x.drop(self.y, 1)
        self.z = lit_dataframe.iloc[:, 5].values
        self.h = lit_dataframe[self.features]
        return self.x, self.y, self.z, self.h

    def file_selector(self):
        file = st.sidebar.file_uploader("choisir un fichier CSV ", type="csv")
        if file is not None:
            dataframe = pd.read_csv(file)
            return dataframe
        else:
            st.text("Veuillez charger un fichier CSV")


    def set_features(self):
        self.features = st.multiselect('Choix des variables',
                                       self.dataframe.columns)

    def properties(self):
        self.type = st.sidebar.selectbox("Type d'algorithme", ("Classification", "Regression", "Clustering"))
        if self.type == "Regression":
            self.chosen_classifier = st.sidebar.selectbox("veuillez choisir l'algorithme",
                                                          ('Random Forest', 'Linear Regression'))
        elif self.type == "Classification":
            self.chosen_classifier = st.sidebar.selectbox("veuillez choisir l'algorithme",
                                                          ('Logistic Regression', 'Naive Bayes'))
            self.Iries_To_Predict = []
            for self.fats in self.features:
                self.h1 = st.slider(str(self.fats), 0, 10)
                self.Iries_To_Predict.append(self.h1)
            #st.write(self.Iries_To_Predict)
        elif self.type == "Clustering":
            pass

    def predict(self, predict_btn):
        if self.type == "Regression":
            if self.chosen_classifier == 'Linear Regression':
                st.markdown("***Prevision OLS***")
                st.markdown('model is **Y = β0 + β1X + ε**.')
                st.write('Y:', self.features[0])
                st.write('X:', self.features[1])
                fig, ax = plt.subplots(figsize=(25,7))
                sns.regplot(x=self.x, y=self.y, fit_reg=True)
                st.pyplot(fig)
                self.X = sm.add_constant(self.x)
                self.model = sm.OLS(self.y, self.X)
                self.results = self.model.fit()
                st.write(self.results.summary())
                self.longueur = st.number_input('Enter a number')
                self.prediction = self.model.predict(self.longueur)[0]
                st.write("Prevision d'une longueur pour une largeur = ", self.longueur)
                st.write(self.prediction)
            if self.chosen_classifier == 'Random Forest':
                from sklearn.datasets import load_iris
                iris = load_iris()
                model = RandomForestClassifier(n_estimators=10)
                model.fit(iris.data, iris.target)
                # Extract single tree
                estimator = model.estimators_[5]

                from sklearn.tree import export_graphviz
                # Export as dot file
                a = export_graphviz(estimator, out_file='tree.dot',
                                feature_names=iris.feature_names,
                                class_names=iris.target_names,
                                rounded=True, proportion=False,
                                precision=2, filled=True)
                st.write(a)
        elif self.type == "Classification":
            if self.chosen_classifier == 'Logistic Regression':
                self.alg = LogisticRegression()
                self.logreg = LogisticRegression(C=1e5)
                self.model = self.logreg.fit(self.h, self.z)
                #self.pred = self.model.predict(self.Iries_To_Predict)

                st.write(np.array(self.Iries_To_Predict).reshape(1, -1))
                self.pred = self.model.predict(np.array(self.Iries_To_Predict).reshape(1, -1))
                st.write(self.pred)



if __name__ == '__main__':
    app = application_stlit()
    app.dataframe = app.file_selector()
    try:
        if app.dataframe is None:
            st.write("chargement de dataframe")

        if app.dataframe is not None:
            st.write(app.dataframe)

        app.set_features()

        if len(app.features) > 1:
            app.properties()
            predict_btn = st.sidebar.button('Predict')
    except(AttributeError ) as e:
        st.markdown('<span style="color:blue">Pas de fichier chargé</span>', unsafe_allow_html=True)

    if app.dataframe is not None and len(app.features) > 1:
        #predict_btn = st.sidebar.button('Predict')
        if predict_btn:
            st.sidebar.text("Progress:")
            my_bar = st.sidebar.progress(0)
            app.prepare_data(app.dataframe)
            app.predict(predict_btn)
            for percent_complete in range(100):
                my_bar.progress(percent_complete + 1)

