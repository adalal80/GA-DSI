import flask
app = flask.Flask(__name__)

#-------- MODEL GOES HERE -----------#

import pandas as pd
import numpy as np


data = pd.read_csv('airbnb_files/neighborhood_revpar_private.csv',index_col=0)

#------ ROUTES GO HERE -------#
# @app.route('/predict', methods=["GET"]) # Two main methods = "GET" & "POST"
# def predict():
#    #bedrooms = flask.request.args['bedrooms']
#    neighborhood = flask.request.args['neighborhood']
#    rent = flask.request.args['rent']

#    rev = data[(data['neighborhood']==neighborhood)]['revpar'].values[0]*365
#    cost = rent*12/2
#    exp_profit = rev - cost
#    results = {'Expected Profits': exp_profit}
#    return flask.jsonify(results)
   

#---------- CREATING AN API, METHOD 2 ----------------#

# This method takes input via an HTML page
@app.route('/page')
def page():
  with open("page.html", 'r') as viz_file:
      return viz_file.read()

@app.route('/result', methods=['POST', 'GET'])
def result():
   '''Gets prediction using the HTML form'''
   if flask.request.method == 'POST':
    inputs = flask.request.form
    neighborhood = inputs['neighborhood']
    rent = inputs['rent']
    try:
      rev = data[(data['neighborhood']==neighborhood)]['revpar'].values[0]*365
      rev = int(rev)
      cost = int(rent)*12/2
      profit = rev - cost
      results = {'Expected Profits for a Private Room on STR in '+str(neighborhood): profit}  
    except:
      results = {'Do not have this Neighborhood in the data, or please check spelling'}
   return flask.jsonify(results)


if __name__ == '__main__':
    '''Connects to the server'''
    HOST = '127.0.0.1'
    PORT = '4000'
    app.run(HOST, PORT)