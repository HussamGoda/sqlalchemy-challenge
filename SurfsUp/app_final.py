# Import Dependencies
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func
import numpy as np
import datetime as dt
from datetime import datetime
import pandas as pd

from flask import Flask, jsonify



# Database Setup

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station


# Flask Setup

app = Flask(__name__)

# Flask Routes
# start the homepage and list all routes

@app.route("/")
def Climate_App():
    return (
        f"Welcome to the Climate Application!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/2011-01-01<br/>"
        f"/api/v1.0/2011-01-01/2014-12-31"
    )

#----------------------------------------------------------------------------------------------------------------------------------------------------
# Convert the query results to a dictionary by using date as the key and prcp as the value.
# Return the JSON representation of your dictionary.


@app.route("/api/v1.0/precipitation")
def percipitation():

    # Create our session (link) from Python to the DB
    session = Session(bind=engine)

    """Return a list of dates and precipitation"""
    # Query all dates and prcp for the last year in the database
    # Find most recent date, and date one year ago
   
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    most_recent_date_seprated = datetime.strptime(most_recent_date[0], "%Y-%m-%d")
    year =  int(most_recent_date_seprated.year)
    month = int(most_recent_date_seprated.month)
    day = int(most_recent_date_seprated.day)
    most_recent_date_updated = dt.date(year, month, day)

    #Date one year ago
    date_one_year_ago = dt.date(year, month, day) - dt.timedelta(days=365)

    # getting required parameters
    parameters_to_retrieve = [Measurement.station, Measurement.date, Measurement.prcp]
    retrive_date_for_one_year = session.query(*parameters_to_retrieve).filter(Measurement.date >= date_one_year_ago).\
        filter(Measurement.date <= most_recent_date_updated).all()

    session.close()

    # Create a dictionary from the row data and assign to a list
    all_dates_prcp = []
    for record in retrive_date_for_one_year:
        dates_prcp_dict = {}
        dates_prcp_dict["date"] = record.date
        dates_prcp_dict["prcp"] = record.prcp
        all_dates_prcp.append(dates_prcp_dict)

    return jsonify(all_dates_prcp)

#----------------------------------------------------------------------------------------
# Return a JSON list of stations from the dataset.
# Returns jsonified data of all of the stations in the database

@app.route("/api/v1.0/stations")
def stations():

    # Create our session (link) from Python to the DB
    session = Session(bind=engine)

    """Merge tables measurement and station"""
    # since the question asks to return all station data, a merge is required as station basic info is in a seperate table 
    # than the prcp and tobs date for each station
     
    columns_names = session.query(Station.id, Station.name, Station.latitude, Station.longitude, Station.elevation,\
                     Measurement.station, Measurement.date, Measurement.prcp, Measurement.tobs).\
                        filter(Station.station == Measurement.station).all()

   
    session.close()

    all_stations_data = []
    
    for cn in columns_names:
        all_stations_data_dict = {}
        all_stations_data_dict["id"] = cn.id
        all_stations_data_dict["station"] = cn.station
        all_stations_data_dict["name"] = cn.name
        all_stations_data_dict["latitude"] = cn.latitude
        all_stations_data_dict["longitude"] = cn.longitude
        all_stations_data_dict["elevation"] = cn.elevation
        all_stations_data_dict["date"] = cn.date
        all_stations_data_dict["prcp"] = cn.prcp
        all_stations_data_dict["tobs"] = cn.tobs
    
        all_stations_data.append(all_stations_data_dict)
  

    return jsonify(all_stations_data)

#-------------------------------------------------------------------------------------------------
# Query the dates and temperature observations of the most-active station for the previous year of data.
# Return a JSON list of temperature observations for the previous year
    
@app.route("/api/v1.0/tobs")
def temps():

    # Create our session (link) from Python to the DB
    session = Session(bind=engine)

    """Return a list of dates and precipitation"""
    # Query all dates and prcp
    
    stations_count = [Measurement.station, func.count(Measurement.station)]

    name_of_most_active_station = session.query(*stations_count).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()
        
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    most_recent_date_seprated = datetime.strptime(most_recent_date[0], "%Y-%m-%d")
    year =  int(most_recent_date_seprated.year)
    month = int(most_recent_date_seprated.month)
    day = int(most_recent_date_seprated.day)
    most_recent_date_updated = dt.date(year, month, day)
    date_one_year_ago = dt.date(year, month, day) - dt.timedelta(days=365)
    
    parameters_to_retrieve = [Measurement.station, Measurement.date, Measurement.tobs]
    temp_data_for_one_year = session.query(*parameters_to_retrieve).filter(Measurement.station==name_of_most_active_station[0]).\
        filter(Measurement.date >= date_one_year_ago).filter(Measurement.date <= most_recent_date_updated).all()

    # Quary Basic Data from the station table for most active station
    columns_data_most_active_stations = [Station.id, Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation]

    basic_data_of_most_active_station = session.query(*columns_data_most_active_stations).filter(Station.station==name_of_most_active_station[0]).all()

    session.close()

    # Create a dictionary from the row data (basic info for most active station)

    most_active_station_basic_info = []
    for basic_data in basic_data_of_most_active_station:
        basic_data_station_active_dict = {}
        basic_data_station_active_dict["id"] = basic_data.id
        basic_data_station_active_dict["station"] = basic_data.station
        basic_data_station_active_dict["name"] = basic_data.name
        basic_data_station_active_dict["latitude"] = basic_data.latitude
        basic_data_station_active_dict["longitude"] = basic_data.longitude
        basic_data_station_active_dict["elevation"] = basic_data.elevation

        most_active_station_basic_info.append(basic_data_station_active_dict)

    # Create a dictionary from the row data (to return dates and tobs as required by the challenge)
    all_temp_data_for_one_year = []
    for station, date, tobs in temp_data_for_one_year:
        temp_date_one_year_dict = {}
        temp_date_one_year_dict["station"] = station
        temp_date_one_year_dict["date"] = date
        temp_date_one_year_dict["tobs"] = tobs

        all_temp_data_for_one_year.append(temp_date_one_year_dict)

#Jsonify both dictionaries
    return jsonify ({"Basic Information for The Most Active Station": most_active_station_basic_info},
                     {"Information for One Year ": all_temp_data_for_one_year})

#-----------------------------------------------------------------------------------------------------

@app.route("/api/v1.0/<start>")

def start_date(start):
    session = Session(bind=engine)

    #accept start date from Url

    date_start_from_url = datetime.strptime(start, "%Y-%m-%d")
    
    columns_to_use = [Measurement.date, Measurement.tobs]  

    calculated_parameters = session.query(*columns_to_use).filter(Measurement.date >= date_start_from_url).filter(Measurement.tobs != "NaN").all()
    session.close()
    
    summary_for_temp = []
    if calculated_parameters:
        temp_min = min(value.tobs for value in calculated_parameters)
        temp_max = max(value.tobs for value in calculated_parameters)
        temp_ave = sum(value.tobs for value in calculated_parameters)/len(calculated_parameters)

       
        summary_for_temp_dict = {}
        summary_for_temp_dict["Minimum Temp."] = temp_min
        summary_for_temp_dict["Maximum Temp."] = temp_max
        summary_for_temp_dict["Average Temp."] = temp_ave
     
        summary_for_temp.append(summary_for_temp_dict)

    
    return jsonify(summary_for_temp_dict)

#------------------------------------------------------------------------------------------------------

@app.route("/api/v1.0/<start>/<end>")
def start_date_end_date(start, end):
    
    session = Session(bind=engine)

    #accept start date and end date from Url
   
    date_start_from_url_1 = datetime.strptime(start, "%Y-%m-%d")
    date_end_from_url = datetime.strptime(end,"%Y-%m-%d")
    
    columns_to_use_1 = [Measurement.date, Measurement.tobs]

    calculated_parameters_1 = session.query(*columns_to_use_1).filter(Measurement.date >= date_start_from_url_1).filter(Measurement.date <= date_end_from_url).filter(Measurement.tobs != "NaN").all()
    session.close()
    
    summary_for_temp_1 = []
    if calculated_parameters_1:
        temp_min = min(value.tobs for value in calculated_parameters_1)
        temp_max = max(value.tobs for value in calculated_parameters_1)
        temp_ave = sum(value.tobs for value in calculated_parameters_1)/len(calculated_parameters_1)

        summary_for_temp_dict_1 = {}
        summary_for_temp_dict_1["Minimum Temp."] = temp_min
        summary_for_temp_dict_1["Maximum Temp."] = temp_max
        summary_for_temp_dict_1["Average Temp."] = temp_ave
     
        summary_for_temp_1.append(summary_for_temp_dict_1)

    
    return jsonify(summary_for_temp_dict_1)  

if __name__ == "__main__":
    app.run(debug=True)
