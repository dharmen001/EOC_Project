from flask import Flask, request, redirect, url_for
import Eoc_Summary
import Eoc_Daily
import Eoc_AdSize
from config import Config

app = Flask(__name__)

form = '''
<html>
   <body>
      
      <form action = "http://localhost:5000" method = "post">
         <p>Enter Name:</p>
         <p><input type = "text" name = "name" /></p>
         <p>Enter id:</p>
         <p><input type = "text" name = "id" /></p>
         <p><input type = "submit" value = "submit" /></p>
      </form>
      
   </body>
</html>
'''

@app.route("/")
def index():
    if request.method == 'GET':
        return form
    elif request.method == 'POST':
        name = request.form['name']
        id = request.form['id']
        return submit(name, id)

@app.route('/submit')
def submit():
    name = request.args.get('name')
    id = request.args.get('id')
    c = Config(name, int(id))

    obj_summary=Eoc_Summary.Summary(c)
    obj_summary.main()
    obj_daily=Eoc_Daily.Daily(c)
    obj_daily.main()
    obj_adSize=Eoc_AdSize.ad_Size(c)
    obj_adSize.main()

    c.saveAndCloseWriter()
    return 'Report Generated'

if __name__ == '__main__':
    app.run()