from flask import Flask, jsonify, request, render_template 
from datetime import date
import datetime
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect, distinct

Hawaii_Data = "sqlite:///Resources/data.sqlite"

def get_db(FILE):
    engine = create_engine(FILE)
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Measurement = Base.classes.measurement
    Station = Base.classes.station
    session = Session(engine)
    return (Measurement, Station, session)

def findDate(date_string):
    formatted_date = pd.to_datetime(date_string, yearfirst=True, infer_datetime_format=True).date()
    formatted_date = formatted_date.strftime("%Y-%m-%d")
    return formatted_date

# Read file and map tables using previously defined function
MS = get_db(Hawaii_Data)[0]
STS = get_db(Hawaii_Data)[1]
sess = get_db(Hawaii_Data)[2]

# Find last reorded day and one year before
last_date = sess.query(MS.date).order_by(MS.date.desc()).first()
last_day = pd.to_datetime(last_date[0], yearfirst=True, infer_datetime_format=True).date()
#  Determine if need to include leap year addition
if (last_day.year % 4 == 0 and last_day.month < 3) or ((last_day.year + 1) % 4 == 0 and last_day.month > 3):
    last_year = last_day - datetime.timedelta(days=366)
else:
    last_year = last_day - datetime.timedelta(days=365)
# Format dates to be used
day1 = findDate(last_day)
day2 = last_year.strftime("%Y-%m-%d")

# Query data using span of the two dates found previously
last_year_data = sess.query(MS).\
    filter(MS.date >= day2).\
    order_by(MS.date.desc()).all()

# Store tob data and dates in lists and create dictionary
tob_data = {}
dates_tob = [x.date for x in last_year_data]
tobs = [x.tobs for x in last_year_data]
for x in range(0, len(dates_tob)):
    tob_data[dates_tob[x]] = tobs[x]

# Query and store precipitation data in dectionary
data = {}

measurement_data = sess.query(MS.date, MS.prcp, MS.station).order_by(MS.date.desc()).all()
for d in measurement_data:
    data[d[0]] = d[1]

# Query and store station data in dictionary
station = {}
for x in range(0, len(measurement_data)):
    L = sess.query(STS.latitude, STS.longitude).filter(STS.station == measurement_data[x][2]).all()
    station[measurement_data[x][2]] = {"lat": L[0][0], "long": L[0][1]}

# Close session in order to open one in start date function - will not work without closing
sess.close()

# Define home page routes in dictionary
routes = {"Precpitation": "/api/v1.0/precipitation", "Stations": "/api/v1.0/stations", "Temperature Observations": "/api/v1.0/tobs",
            "Temperatures for given date and end date (Between 01/01/2010 and 2017/08/23)": "/api/v1.0/<start>",
            "Temperatures for span between given dates (Between 01/01/2010 and 2017/08/23)": "/api/v1.0/<start>/<end>"}

# -----------------------------------------------------
# Set up flask applications
app = Flask(__name__)

# Home page - record of other pages/endpoints
@app.route("/")
def home():
    print("Server received request for 'Home' page...")
    app.config["JSON_SORT_KEYS"] = False
    return jsonify(routes)
    
# Precipitation data
@app.route("/api/v1.0/precipitation")
def precipitation():
    app.config["JSON_SORT_KEYS"] = False
    return jsonify(data)

# Station data
@app.route("/api/v1.0/stations")
def stations():
    return jsonify(station)

# Temperature observation data
@app.route("/api/v1.0/tobs")
def tobs():
    app.config["JSON_SORT_KEYS"] = False
    return jsonify(tob_data)

# Uses endpoint and determines whether just one date or two, returns temperature data for that span
# If one date, span between given date and last recorded date
# If two dates, span between the two dates
@app.route("/api/v1.0/<path:dates>")
def startEnd_date(dates):
    dates = dates.replace(" ", "")

    # Determine if one date or two dates are used by splitting using '/'
    split_dates = dates.split("/")
    
    # If there are more than 3 split values, there are two dates: XX/XX/XX/ZZ/ZZ/ZZ
    if len(split_dates) > 4:
        start = split_dates[0] + '/' + split_dates[1] + '/' + split_dates[2]
        end = split_dates[3] + '/' + split_dates[4] + '/' + split_dates[5]
    # If there are 4 split instances, there are two dates with one using the XX/XX/XX format
    elif len(split_dates) == 4:
        if len(split_dates[0]) > 2:
            start = split_dates[0]
            end = split_dates[1] + '/' + split_dates[2] + '/' + split_dates[3]
        else:
            start = split_dates[0] + '/' + split_dates[1] + '/' + split_dates[2]
            end = split_dates[3]
    # If there are three or one split values, there is only one date: xx/xx/xx or xx-xx-xx or monthXX,XXXX
    elif len(split_dates) == 3 or len(split_dates) == 1:
        start = dates
        end = pd.to_datetime(day1, yearfirst=True, format='%Y/%m/%d').date()
        end = end.strftime("%m-%d-%Y")
    # If there are only two split instances, there are two dates suing either a '-' format or full string format: XX-XX-XX or month,XX,XXXX
    elif len(split_dates) == 2:
        start = split_dates[0]
        end = split_dates[1]

    # Remove separators from dates
    start = start.replace('/', '')
    start = start.replace(',', '')
    start = start.replace('-', '')
    end = end.replace('/', '')
    end = end.replace(',', '')
    end = end.replace('-', '')

    try:
        try:
            start_date = pd.to_datetime(start, yearfirst=True, format='%m%d%y').date()
            start_date = start_date.strftime("%Y-%m-%d")
        except:
            start_date = pd.to_datetime(start, yearfirst=True, format='%m%d%Y').date()
            start_date = start_date.strftime("%Y-%m-%d")
    except:
        try:
            start_date = pd.to_datetime(start, yearfirst=True, format='%b%d%Y').date()
            start_date = start_date.strftime("%Y-%m-%d")
        except:
            start_date = pd.to_datetime(start, yearfirst=True, format='%B%d%Y').date()
            start_date = start_date.strftime("%Y-%m-%d")

    try:
        try:
            end_date = pd.to_datetime(end, yearfirst=True, format='%m%d%y').date()
            end_date = end_date.strftime("%Y-%m-%d")
        except:
            end_date = pd.to_datetime(end, yearfirst=True, format='%m%d%Y').date()
            end_date = end_date.strftime("%Y-%m-%d")
    except:
        try:
            end_date = pd.to_datetime(end, yearfirst=True, format='%b%d%Y').date()
            end_date = end_date.strftime("%Y-%m-%d")
        except:
            end_date = pd.to_datetime(end, yearfirst=True, format='%B%d%Y').date()
            end_date = end_date.strftime("%Y-%m-%d")
            
    # Read data and store in defined tables
    MS = get_db(Hawaii_Data)[0]
    STS = get_db(Hawaii_Data)[1]
    sess = get_db(Hawaii_Data)[2]

    # Using dates defined above from endpoint, query temperature information dictionary to post
    tempMin = sess.query(func.min(MS.tobs)).filter(MS.date.between(start_date, end_date)).all()[0][0]
    tempMax = sess.query(func.max(MS.tobs)).filter(MS.date.between(start_date, end_date)).all()[0][0]
    tempAvg = sess.query(func.avg(MS.tobs)).filter(MS.date.between(start_date, end_date)).all()[0][0]
    averageTemps = {"Dates": (start_date, end_date), "Minimum": tempMin, "Maximum": tempMax, "Average": round(tempAvg, 2)}
    # Close session
    sess.close()

    # Setting to preserve dictionary order with jsonify
    app.config["JSON_SORT_KEYS"] = False
    return jsonify(averageTemps)

if __name__ == "__main__":
    app.run(debug=True)