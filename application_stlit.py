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
from sklearn.impute import SimpleImputer
import sys
from pandas.errors import ParserError
import time
import statsmodels.api as sm

st.title('Machine Learning AMARIS example')

class application_stlit:
    def prepare_data(self, lit_dataframe):
        self.feat_y = self.features[0]
        self.y = lit_dataframe[[self.feat_y]]
        self.feat_x = lit_dataframe[self.features]
        self.x = self.feat_x.drop(self.y, 1)
        return self.x , self.y

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
        elif self.type == "Clustering":
            pass

    def predict(self, predict_btn):
        if self.type == "Regression":
            if self.chosen_classifier == 'Linear Regression':
                st.write("Prevision OLS")
                fig, ax = plt.subplots(figsize=(25,7))
                sns.regplot(x=self.x, y=self.y, fit_reg=True)
                st.pyplot(fig)
                #self.model_linReg = LinearRegression()
                #self.model = self.model_linReg.fit(self.x, self.y)
                #self.precision = self.model_linReg.score(self.x, self.y)
                #self.longueur = 2.5
                #self.prediction = self.model_linReg.predict([[self.longueur]])
                #st.write(self.precision * 100)
                #st.write(self.prediction)
                self.X = sm.add_constant(self.x)
                self.model = sm.OLS(self.y, self.X)
                self.results = self.model.fit()
                st.write(self.results.summary())
                self.longueur = 2.5
                self.prediction = self.model.predict(self.longueur)[0]
                st.write("Prevision pour une longueur = 2.5")
                st.write(self.prediction)

        elif self.type == "Classification":
            if self.chosen_classifier == 'Logistic Regression':
                self.alg = LogisticRegression()
                self.Y = app.dataframe.iloc[:, 5].values
                self.X = app.dataframe.iloc[:, [1,2,3, 4]].values
                self.logreg = LogisticRegression(C=1e5)
                self.model = self.logreg.fit(self.X, self.Y)
                self.Iries_To_Predict = [
                    [5.5, 2.5, 5.5, 6.5],
                    [7, 3, 5, 6]
                ]
                self.pred = self.model.predict(self.Iries_To_Predict)
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

