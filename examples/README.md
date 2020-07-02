# Examples
This folder contains Jupyter notebooks and HTML files with examples of how to
load, analyze and visualize the
[Open COVID-19 dataset](https://github/open-covid-19/data) dataset. You can
use Google Colab if you want to run your analysis using notebooks without
having to install anything in your computer, simply go to this URL:
https://colab.research.google.com/github/open-covid-19/data.

See below for a list and description of some of the files in this folder.

#### [Data Loading Jupyter Notebook](data_loading.ipynb)
This notebook contains very basic examples of how to load and filter data
from the Open COVID-19 dataset using `pandas`.

#### [Data Loading HTML Page](data_loading.html)
This HTML file contains the bare minimum needed to load the data from the
Open COVID-19 dataset and display it in a table using `jquery`.

#### [Category Estimation Jupyter Notebook](category_estimation.ipynb)
This notebook showcases a methodology for estimating current mild, severe and
critical patients by applying empirical data recorded in literature and
validating the results against reported.

#### [Exponential Modeling Jupyter Notebook](exponential_modeling.ipynb)
This notebook explores modeling the spread of COVID-19 confirmed cases as an
exponential function. While this is not a good model for long or even
medium-term predictions, it is able to fit initial outbreaks quite well. For a
more sophisticated and accurate model, see the
[logistic modeling](logistic_modeling.ipynb) notebook.

#### [Logistic Modeling Jupyter Notebook](logistic_modeling.ipynb)
This notebook explores modeling the spread of COVID-19 confirmed cases as a
logistic function. It compares the accuracy of two sigmoid models:
[simple logistic function](https://en.wikipedia.org/wiki/Logistic_function)
and [Gompertz function](https://en.wikipedia.org/wiki/Gompertz_function), and
finds the Gompertz function to be a fairly accurate short-term predictor of
future confirmed cases.
