import os
import pandas as pd 
import numpy as np
from datetime import datetime as dt, timedelta as td


def create_single_sensor_df(station_nr, raw_data_path, 
                            start_datetime =dt(2024,5,26,12), 
                            end_datetime =dt(2024,5,26,14),ffil_limit=0):
    
    infile = os.path.join(raw_data_path, f"device_{station_nr}_rain.csv")
    station_df = pd.read_csv(infile, index_col="time_iso", 
                usecols=["time_iso", "value"],
                parse_dates=True, date_format="ISO8601")

    
    
    #station_df["time_iso"]= station_df["time_iso"].dt.floor("Min")
    
    # set index to minutes
    station_df.index = station_df.index.floor("min")
    
    # remove timezone-localisation
    station_df.index = station_df.index.tz_localize(None)
    
    # take max-values on those minutes
    station_df  = station_df.groupby(["time_iso"]).max()

    # Create new Timerange for new indices
    timerange = pd.date_range(start=start_datetime, 
                              end=end_datetime,
                              freq="min")

    # create filled station df with variables.
    # Time-indices with no values will get a value of NaN assigned
    station_df = station_df.reindex(timerange, fill_value=np.nan)
    
    
    # create filled df. 
    # fill NaN-Values of previous df via ffill (earlier value is taken)
    if ffil_limit > 0:
        station_df = station_df.ffill(limit=ffil_limit)
    
    # resample to 5min steps in index
    station_df =  station_df.resample('5min', closed='right', label='right').max()
    
    return station_df
    # add values to sensors_dict
    
    
def create_full_sensor_df(active_sensors: list, allocations: dict, folderpath: str):
    """
    Generate accumulated Sensor csv files and dataframes from allocation dict and active sensors.

    Args:
        active_sensors (list): List of active Sensors
        allocations (dict): allocations for Sensor-Names to devEuis
        
    """

    print("==== CREAtING SENSOR DF ====")
    # create empty dicts
    dict_sensors = {}
    dict_sensors_nan = {}
    
    # iterate over stations
    for station_id in active_sensors:
        station_nr = str(station_id)
        infile = os.path.join(folderpath, f"device_{station_nr}_rain.csv")
        station_df = pd.read_csv(infile, index_col="time_iso", 
                    usecols=["time_iso", "device_eui", "device_id", "value"],
                    parse_dates=True, date_format="ISO8601")

        #station["time_iso"]= station["time_iso"].dt.floor("Min")
        station_df.index.floor("min")

        # Find minute duplicates
        #station_df[station_df.index.floor("min").duplicated()]

        # keep only max values
        new_station_df = station_df.copy()
        
        # keep only minute values for the index
        new_station_df.index = new_station_df.index.floor("min")
        
        # take max-values on those minutes
        new_station_df_maxed = new_station_df.groupby(["time_iso"]).max()

        # Create new Timerange for new indices
        timerange = pd.date_range(start=new_station_df_maxed.index.min(), 
                            end=new_station_df_maxed.index.max() + td(minutes=1),
                            freq="min")

        # get the deveui of current station
        deveui=allocations[station_id]

        # create filled station df with variables.
        # Time-indices with no values will get a value of NaN assigned
        station_filled_nan = new_station_df_maxed.reindex(timerange, fill_value=np.nan)
        
        # add values to the dict with nan values
        dict_sensors_nan[deveui]=station_filled_nan["value"]
        
        # create filled df. 
        # fill NaN-Values of previous df via ffill (earlier value is taken)
        # TODO add limit-Param to ffill (for example after 10 minutes zero it)
        station_filled = station_filled_nan.ffill()
        
        # resample to 5min steps in index
        station_5min =  station_filled.resample('5min', closed='right', label='right').max()
        
        # add values to sensors_dict
        dict_sensors[deveui]=station_5min["value"]

    # create merged df from sensors
    df_merged = pd.DataFrame(dict_sensors).replace(255,0)
    df_merged.index = df_merged.index.tz_localize(None)
    df_merged.index.rename("time")

    # create output_csv
    df_merged.to_csv(ALL_SENSOR_DATA, index=True, index_label="time")

    return df_merged


        
        
if __name__ == "__main__":
    create_single_sensor_df(13401,  r"DATA\\last6months_140824")