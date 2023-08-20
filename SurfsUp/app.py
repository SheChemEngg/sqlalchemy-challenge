# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt

# Ignore SQLITE warnings related to Decimal numbers in the Hawaii database
import warnings
warnings.filterwarnings('ignore')

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify, request

#################################################
# Database Setup
#################################################

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine, reflect = True)

# Save references to each table

# Save a reference to the measurement table as `Measurement`
Measurement = Base.classes.measurement

# Save a reference to the station table as `Station`
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
            f"<br/>"
            f"Welcome to Honolulu County, Hawaii Weather Data API!<br/>"
            f"<br/>"
            f"Weather-Information Routes:<br/>"
            f"<br/>"
            f"/api/v1.0/precipitation<br/>"
            f"/api/v1.0/stations<br/>"
            f"/api/v1.0/most_active_station_temperatures<br/>"
            f"/api/v1.0/temperature_stats/Choose_date_on_or_after/2010-01-01<br/>"
            f"/api/v1.0/temperature_stats/Choose_date_on_or_between/2010-01-01/2017-08-23"
            )

       
@app.route("/api/v1.0/precipitation")
def precipitation():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a dictionary {'date': 'prcp'} for the last 12 months"""
    from datetime import datetime, timedelta

    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    print(most_recent_date)

    most_recent_date_str = most_recent_date[0]
    most_recent_date = datetime.strptime(most_recent_date_str, '%Y-%m-%d')
    one_year_ago = most_recent_date - timedelta(days=366)


    # Query to retrieve the date and precipitation scores
    date_prcp = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= one_year_ago, Measurement.date <= most_recent_date).all()

    # Save the query results as a Pandas DataFrame
    df = pd.DataFrame(date_prcp, columns=['date', 'prcp'])

    # Sort the DataFrame by date
    date_sorted_df = df.sort_values('date')

    session.close()

    # Convert the DataFrame to a dictionary with 'date' as keys and 'precipitation' as values
    date_prcp_dict = date_sorted_df.set_index('date')['prcp'].to_dict()


    return jsonify(f'2016-8-23 to 2017-8-23 : Precipitation(in)', date_prcp_dict)



@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all station names"""
    # Query for the station names using the group_by function
    station_list = session.query(Measurement.station).group_by(Measurement.station).all()

    # Convert the results to a list of station names
    station_ls = [station[0] for station in station_list]
    
    session.close()

    # Return the list as JSON
    return jsonify({"Station Names  ":  station_ls})


@app.route("/api/v1.0/most_active_station_temperatures")
def most_active_station_temperatures():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of temperatures for the most active station over the last 12 months"""
    # query to find the most active station

    station_counts = session.query(Measurement.station, func.count(Measurement.station)).\
                     group_by(Measurement.station).\
                     order_by(func.count(Measurement.station).desc()).all()
    
    most_active_station = station_counts[0][0]
    
    from datetime import datetime, timedelta

    #Last recorded date
    station_date_last = session.query(Measurement.date).\
                        order_by(Measurement.date.desc()).\
                        filter(Measurement.station == most_active_station).first()

    station_date_last_str = station_date_last[0]
    station_date_last = datetime.strptime(station_date_last_str, '%Y-%m-%d')

    # Date a year ago
    station_date_year_ago = station_date_last - timedelta(days=366)

    # Query the "tobs" data for the last year
    tobs_data = session.query(Measurement.tobs).\
                filter(Measurement.station == most_active_station,
                    Measurement.date >= station_date_year_ago,
                    Measurement.date <= station_date_last).all()

    # Extract the temperatures from tobs_data
    temperatures = [int(temp[0]) for temp in tobs_data]
        

    session.close()

    # Return the list as JSON
    return jsonify(f'Observed temperatures for the most active station,  USC00519281 :     {temperatures}')



@app.route("/api/v1.0/temperature_stats/Choose_date_on_or_after/<start_date>")
@app.route("/api/v1.0/temperature_stats/Choose_date_on_or_between/<start_date>/<end_date>")
def temperature_stats(start_date, end_date="2017-08-23"):

    from datetime import datetime

    # Convert to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    if start_date > end_date or start_date < datetime(2010, 1, 1) or end_date > datetime(2017, 8, 23):
        return jsonify({"error": f"Choose dates from 2010-01-01 to 2017-08-23."}), 404
    
    elif start_date > end_date or start_date > datetime(2017, 8, 23) or end_date < datetime(2010, 1, 1):
        return jsonify({"error": f"Choose dates from 2010-01-01 to 2017-08-23."}), 404
        
    else:
        # Query temperature statistics within the date range
        temperature_stats = session.query(func.min(Measurement.tobs),
                                            func.max(Measurement.tobs),
                                            func.round(func.avg(Measurement.tobs), 2)).\
                                            filter(Measurement.date >= start_date,
                                            Measurement.date <= end_date).all()

        # Extract the statistics values from the result tuple
        min_temp, max_temp, avg_temp = temperature_stats[0]

        session.close()

        # Return the statistics as JSON
        return jsonify(f'Start Date: {start_date.strftime("%Y-%m-%d")}, End Date: {end_date.strftime("%Y-%m-%d")}', 
                        {
                        "A. Start date  ": start_date.strftime("%Y-%m-%d"),
                        "B. End date    ": end_date.strftime("%Y-%m-%d"),
                        "C. Minimum temperature (F)": min_temp,
                        "D. Maximum temperature (F)": max_temp,
                        "E. Average temperature (F)": avg_temp
                        })



if __name__ == '__main__':
    app.run(debug=True)




