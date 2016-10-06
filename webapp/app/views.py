# jsonify creates a json representation of the response
from flask import jsonify

from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster
from flask import render_template
from flask import request



# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster(['52.34.52.190','54.70.79.144','52.40.181.97','52.10.147.196'])

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('magic_number')

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

@app.route("/topuser", methods=['POST'])
def index_post():
    date = request.form["date"]
    stmt = "SELECT * FROM user_spend WHERE date=%s ORDER BY friend_spend DESC LIMIT 10"
    response = session.execute(stmt, parameters=[date])
    topuser_response_list = []
    user_id_list = []
    friend_list = []
    for val in response:
        topuser_response_list.append(val)
    topuserresponse = [{"date": str(x.date), "passenger_id": x.passenger_id, "self_spend": x.self_spend, "friend_spend": x.friend_spend} for x in topuser_response_list]

    for each in topuser_response_list:
        user_id_list.append(each[2])
    topuser_spend_list = []

    for user in user_id_list:
        q = "SELECT self_spend from user_spend2 where date=%s AND passenger_id=%s"
        response2 = session.execute(q, parameters=[date, user])
        topuser_spend_list.append(response2[0])
    topuserspend = [{"self_spend": x.self_spend} for x in topuser_spend_list]

    return render_template("topuser.html", output=topuserresponse, output2=topuserspend)

@app.route('/slides')
def slides():
    return render_template("slides.html")

@app.route('/realtime')
def realtime():
 return render_template("realtime.html")