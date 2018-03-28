from flask import Flask, request, redirect, url_for, make_response, jsonify
import flask_excel as excel
import Eoc_Summary
import Eoc_Daily
import Eoc_AdSize
import Eoc_Video
import Eoc_Intraction
import EOC_definition
from config import Config

app = Flask(__name__)

form = '''
<html>
    <h1 align = "center">
        <font color = white>
         Exponential Reporting System</font> </h1>
         <body bgcolor="#00B0F0">
         <a href='https://photos.google.com/share/AF1QipMAS8dqCXrLSPhvLawmo4oPh74bj3Xn-k_grYXr7I_gg13Qte1tPeP0DacLlnSDzQ?key=aTdUbVNmNTM0d2ZSdFk3bm1fYnZIYjNVZGhKX0VB&source=ctrlq.org'><img src='https://lh3.googleusercontent.com/vd-__MRdchkwumqg0e5tGSULQ6I9JhPssaMRQMWmaLQuupwhlYlcizgLUkW73KcfZ6XBEnxD0KFrjk2JnOel49ki4cpHLUVIYhZdRLLQO847HxygWO2bnU3iP6Ep1VwNuZ93r2uT0w=w2400'/></a>
         <form action = "http://localhost:5000/" method="POST">
         <p>Enter Name:</p>
         <p><input type = "text" name = "name" /></p>
         <p>Enter id:</p>
         <p><input type = "text" name = "id" /></p>
         <p><input type = "submit" value = "submit" /></p>
      </form>
   </body>
</html>
'''

@app.route("/", methods =['GET','POST'])
def index():
    if request.method == 'GET':
        return form
    elif request.method == 'POST':
        name = request.form['name']
        id = int(request.form['id'])
        return submit(name, id)



@app.route('/submit')
def submit(name, id):
    #name = request.args.get('name')
    #id = int(request.args.get('id'))
    c = Config(name, id)
    obj_summary = Eoc_Summary.Summary(c)
    obj_summary.main()
    obj_daily=Eoc_Daily.Daily(c)
    obj_daily.main()
    #obj_adSize=Eoc_AdSize.ad_Size(c)
    #obj_adSize.main()
    #obj_Video=Eoc_Video.Video(c)
    #obj_Video.main()
    #obj_Intraction=Eoc_Intraction.Intraction(c)
    #obj_Intraction.main()
    obj_definition=EOC_definition.definition(c)
    obj_definition.main()
    c.saveAndCloseWriter()
    return 'Report Generated'

if __name__ == '__main__':
    app.run()