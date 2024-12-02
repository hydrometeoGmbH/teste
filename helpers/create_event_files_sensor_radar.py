from datetime import datetime as dt, timedelta as td
import geopandas as gpd
import glob
import json
import netCDF4
import numpy as np
import os
import pandas as pd
import requests




# Define GLOBALS
RAW_SENSOR_DATA = r"E:\\Python Philipp\\Heavy Rain\\0_create_event_files\\last6months_140824"
SENSORS_ACTIVE_FILE = r"E:\\Python Philipp\\Heavy Rain\\0_create_event_files\\sensorsLocations_pixel_FID.csv"
ALL_SENSOR_DATA = r"E:\\Python Philipp\\Heavy Rain\\0_create_event_files\\all_sensor_data.csv"
FILEPATH_RADAR = r"E:\\Python Philipp\\Heavy Rain\\0_create_event_files\\radar_files"  # SCU-Folderpath
FILEPATH_RADAR_DIR = r"E:\\Python Philipp\\Heavy Rain\\0_create_event_files\\radar_outfiles_dir"
MIN_DATE = r"2024-05-26 12:00:00" #yyyy-mm-dd HH:MM:SS
MAX_DATE = r"2024-05-26 14:20:00"
EVENT_NAME = r"random_event"


SENSORS_PIXELID={'E1E54': 842, 
                'E223A': 772, 
                'E3C10': 741, 
                'E434E': 770, 
                'E436B': 943, 
                'E45DB': 706, 
                'E45EC': 943, 
                'E45ED': 710, 
                'E45F8': 1100, 
                'E4692': 671, 
                'E46BE': 735, 
                'E4A7A': 771, 
                'E4AC1': 639, 
                'E4DB5': 805, 
                'E4E8B': 773, 
                'E4EB2': 704, 
                'E5217': 804, 
                'E525C': 871, 
                'E5268': 807, 
                'E52A7': 1042, 
                'E53B4': 1167, 
                'E557C': 773, 
                'E570D': 780, 
                'E572D': 1073, 
                'E5744': 740, 
                'E57C3': 837, 
                'E57D3': 803, 
                'E5B98': 941, 
                'E5B9D': 768,
                'E5BA7': 670,
                'E5FC8': 770,
                'E603E': 840,
                'E6040': 873,
                'E6069': 906,
                'E611E': 634,
                'E6120': 736,
                'E6A24': 802,
                'E7052': 774,
                'E7217': 768,
                'E72F2': 1136,
                'E73A3': 771,
                'E75A1': 1071,
                'E761F': 773,
                'E7A34': 705,
                'E7B54': 773,
                'E7DE6': 740}

def main():
    """
    main function
    """ 
    # Load in list of active sensors
    print(f"{'='*30} GLOBALS {'='*30}")
    for key in globals():
        print(key, ":", globals()[key])
    print("\n\n")
    
    
    sensors_active = pd.read_csv(SENSORS_ACTIVE_FILE, sep=";")
    listSensorActive = list(sensors_active["Stationsnr"])

    # create allocation dict allocating deveui to SensorID
    alloc_dict = {}
    for i in range(len(sensors_active)):
        value = sensors_active.iloc[i][0]
        key = int(sensors_active.iloc[i][6])
        alloc_dict[key] = value

    # create and save all sensor data
    
    if input("create new file for sensor data? (y/n)").lower() == "y":
        full_sensor_df = create_full_sensor_df(listSensorActive, alloc_dict)

    else: 
        full_sensor_df = pd.read_csv(ALL_SENSOR_DATA, index_col="time", sep=",")

    if input("create new event radar files (y/n)").lower() == "j":
        print(f"importing radar files to {FILEPATH_RADAR}")
        get_radar_files()

    data_dicts = create_timeseries()
    
    # TODO if data_dicts exists --> do not import csv from file
    radar_data_folder = FILEPATH_RADAR_DIR
    radar_files = os.listdir(radar_data_folder)
    #full_sensor_df = r"all_sensor_data.csv"
    for file in radar_files:

        radar_event_file = os.path.join(radar_data_folder, file)
        out_folder = EVENT_NAME
        if not os.path.isdir(out_folder):
            os.mkdir(out_folder)
        
        filename = f"{EVENT_NAME}_{file.split('.')[0].split('_')[-1]}"
        out_subfolder = os.path.join(out_folder, filename)
        print("creating event file in", out_folder)
        create_event_files(full_sensor_df, radar_event_file,
                           out_subfolder, EVENT_NAME)


# =============================== SENSOR FUNCTIONS ==============================
def create_event_files(sensor_data, radar_data, out_folder, event_name):
    """
    creates event files for specific rain events with radar and sensor data next to each other:
        - {event_name}_unmoved.csv: radar and sensor data unmoved next to each other 
                                    for each sensor in the event timeframe
        - {event_name}_5min.csv: same as <...unmoved>, but sensor data is moved 5 minutes 
                                 later to account vor rainfall time
        - {event_name}_correlations.csv: correlations between sensor and radar data
                                         for each station. 
                                         Pearson, Kendall and Spearman-Corr are listed
        

    Args:
        sensor_data (pd.DataFrame | str): the full sensor-Data or filestring to csv
                                          containing to(has to include timeframe of rain event)
        radar_data (pd.DataFrame | str): the radar-data for the rain-event or filestring to csv
                                         containing it. (has to be only for rain event)
        out_folder (str): folderstring the output directory should be named
        event_name (str): name of rain_event (names output files)

    Raises:
        TypeError: Wrong data type in sensor data
        TypeError: Wrong data type in radar data
    """
    # load in data
    if isinstance(sensor_data, str):
        sensor_data = pd.read_csv(sensor_data, index_col="time", parse_dates=True)
        #sensor_data = sensor_data.set_index(pd.DatetimeIndex(sensor_data["time"]), drop=False, inplace=True)
    elif not isinstance(sensor_data,  pd.DataFrame):
        raise TypeError("FALSCHER DATATYPE: Sensor-Data")

    try:
        if isinstance(radar_data, str):
            event_radar_df = pd.read_csv(radar_data, sep=";", parse_dates=True, index_col="time")
        elif isinstance(radar_data, pd.DataFrame):
            event_radar_df = radar_data
        else :
            raise TypeError("FALSCHER DATATYPE DER RADAR-DATA")
    except ValueError as exc:
        if isinstance(radar_data, str):
            event_radar_df = pd.read_csv(radar_data, sep=",", parse_dates=True, index_col="time")
        elif isinstance(radar_data, pd.DataFrame):
            event_radar_df = radar_data
        else :
            raise TypeError("FALSCHER DATATYPE DER RADAR-DATA") from exc

    # delocalize the datetime series in indices
    sensor_data.set_index(pd.DatetimeIndex(sensor_data.index), drop=False, inplace=True)
    sensor_data.index = sensor_data.index.tz_localize(None)
    event_radar_df.index = event_radar_df.index.tz_localize(None)

    # create spliced sensor df
    start = event_radar_df.index[0]
    end = event_radar_df.index[-1]
    print(start)
    print(end)

    # create an input to ask the user if he/she wants to slice date for an specific period
    time_window= input(f"Radar time window between {start} and {end}, would like to change?" \
                        "(reply with yes or no)")

    if time_window=='yes':
        start=input("Define the start date as string following this format: 2024-05-26 00:00:00")
        end= input("Define the end date as string following this format: 2024-05-26 23:55:00")
        event_radar_df=event_radar_df[start:end]
    else:
        pass
    event_sensor_df = sensor_data[start:end]

    # create 5 min displaced dfs
    # Moves Sensor time to 5mins later
    event_radar_df_5min = event_radar_df.iloc[:-1,:]
    event_sensor_df_5min = event_sensor_df.iloc[1:,:]
    event_sensor_df_5min.index = [index for index in event_radar_df_5min.index]
    event_sensor_df_5min.index = event_sensor_df_5min.index.rename("time")

    # merge to event_dfs
    event_df = event_sensor_df.merge(event_radar_df, how="left", on="time",
                                     suffixes=("_sensor", "_radar"))
    event_df_5min = event_sensor_df_5min.merge(event_radar_df_5min, how="left",
                                               on="time", suffixes=("_sensor", "_radar"))

    # sort df columns
    event_df = event_df.reindex(sorted(event_df.columns), axis=1)
    event_df_5min = event_df_5min.reindex(sorted(event_df_5min.columns), axis=1)

    # create filestrings
    out_e = os.path.join(out_folder, f"{event_name}_unmoved.csv")
    out_e_5min = os.path.join(out_folder, f"{event_name}_5min.csv")
    out_corr = os.path.join(out_folder, f"{event_name}_correlations.csv")

    # create folder if needed
    if not os.path.isdir(out_folder):
        os.mkdir(out_folder)

    # create out_csvs for event and event_5min
    print(f"saving csv files to {out_folder}", end="...")
    event_df.to_csv(out_e, index=True, index_label="time")
    event_df_5min.to_csv(out_e_5min, index=True, index_label="time")

    # create correlation-df and output csv    
    corr_df = create_corr_df(event_df, event_df_5min)
    corr_df.to_csv(out_corr, index=False)
    print("done")



def create_corr_df(event_df, event_df_5min):
    """turns the event data into a correlation df

    Args:
        event_df (pd.DataFrame): event_df created in <create_event_files()>
        event_df_5min (_type_): 5min moved event_df created in <create_event_files()>

    Returns:
        pd.DataFrame: DataFrame consisting of correlations for the rain event for each station
    """
    # read in sensors
    sensors_active=pd.read_csv(SENSORS_ACTIVE_FILE, sep=";")

    # create empty df
    correlations_df = pd.DataFrame(columns=["deveui", "pearson", "pearson_5min",
                                        "kendall", "kendall_5min",
                                        "spearman", "spearman_5min"])

    # create individual correlation coefficients
    for deveui in sensors_active["devEui"]:

        # Deveui Series for Sensor & Radar
        series_Sensor = event_df[deveui+"_sensor"]
        series_Radar = event_df[deveui+"_radar"]
        series_Sensor_5min = event_df_5min[deveui+"_sensor"]
        series_Radar_5min = event_df_5min[deveui+"_radar"]

        # make value 1 for full 0 Data of both series
        if (sum(series_Sensor.values) == 0) and (sum(series_Radar.values)==0):
            print("NA DETECTED")
            pearson = kendall = spearman = pearson_5min = kendall_5min = spearman_5min = 1

        else:
            # unmoved corr
            pearson = round(series_Sensor.corr(series_Radar, method="pearson"), 3)
            kendall = round(series_Sensor.corr(series_Radar, method="kendall"), 3)
            spearman = round(series_Sensor.corr(series_Radar, method="spearman"), 3)

            # corr moved by 5min
            pearson_5min = round(series_Sensor_5min.corr(series_Radar_5min, method="pearson"), 3)
            kendall_5min = round(series_Sensor_5min.corr(series_Radar_5min, method="kendall"), 3)
            spearman_5min = round(series_Sensor_5min.corr(series_Radar_5min, method="spearman"), 3)

        # create current correlations as df
        curr_corr = pd.DataFrame({"deveui": [deveui], 
                              "pearson": [pearson], "pearson_5min": [pearson_5min], 
                              "kendall": [kendall], "kendall_5min": [kendall_5min],
                              "spearman": [spearman], "spearman_5min": [spearman_5min]})

        # append df to entire df
        correlations_df = pd.concat([correlations_df, curr_corr], ignore_index=True)

    return correlations_df



def create_full_sensor_df(active_sensors: list, allocations: dict):
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
        infile = os.path.join(RAW_SENSOR_DATA, f"device_{station_nr}_rain.csv")
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

# =============================== RADAR FUNCTIONS ==============================


def get_radar_files(start_datetime=MIN_DATE, 
                    end_datetime=MAX_DATE, 
                    filepath_radar_files=FILEPATH_RADAR):
    
    """
    Request bo-i-t API for Radar-data for specific time window.
    Writes this data into files into filepath_radar_files.
    One file for every 5min is created in the folder

    Args:
        start_datetime (_str_, optional): Start datetime as string ('yyyy-mm-dd HH:MM:SS') 
                                          of searched rain event.
                                          If not specified, defaults to global MIN_DATE.
        end_datetime (_str_, optional): Start datetime as string ('yyyy-mm-dd HH:MM:SS') 
                                        of searched rain event.
                                        If not specified, defaults to MAX_DATE.
        filepath_radar_files (_type_, optional): filetype for the outfiles generated in this function. 
                                                 Defaults to FILEPATH_RADAR.

    """
    #TODO
    #   interne Datenbankabfrage
    
    # Request to bo-i-t
    print("requesting List...",end="...")
    rad_area = "BOO"  # BOO oder NRW
    min_date = start_datetime  # yyyy-mm-dd HH:MM:SS
    max_date = end_datetime
    reqUrl = f"https://bo-i-t.selfhost.eu/heavy_rain/api/Radar_Data?minDate={min_date}&maxDate={max_date}&region={rad_area}"
    headersList = {
        "Accept": "/",
        "User-Agent": "Thunder Client (http://www.thunderclient.com)",
        "access_token": "z(A@vpTqnf~Oh}yZ|:2Do1s$h)(7XR=,F{*g;EGop|8=vV:3I/0w:g}Uy9yeB-$)"
    }
    payload = ""
    response = requests.request("GET",
                                reqUrl,
                                data=payload,
                                headers=headersList,
                                timeout=60
                                )
    print("...done")
    
    # Der Rückgabewert kommt als String, der aussieht wie eine Liste.
    # Den String in eine Liste umwandeln
    mylist = json.loads(response.text)

    # save files request single files and save to folder
    print("checking stored files for matches and writing new ones")
    for radfile in mylist:
        
        # see local path if file is already saved (no new download needed)
        if radfile in os.listdir(filepath_radar_files):
            print(f"file {radfile} already in Database. Continuing")
            continue
        
        # request for specific file at bo-i-t
        print(f"requesting {radfile}")
        reqUrl = f"https://bo-i-t.selfhost.eu/heavy_rain/api/Radar_Data_one_file?file_name={radfile}&region={rad_area}"
        headersList = {
            "Accept": "/",
            "User-Agent": "Thunder Client (http://www.thunderclient.com)",
            "access_token": "z(A@vpTqnf~Oh}yZ|:2Do1s$h)(7XR=,F{*g;EGop|8=vV:3I/0w:g}Uy9yeB-$)"
        }
        payload = ""
        response2 = requests.request("GET",
                                    reqUrl,
                                    data=payload,
                                    headers=headersList,
                                    timeout=60
                                    )
        
        # get content from response
        binary_data = response2.content
        
        # write data from binary into file
        print("wirting file", end="...")
        file_path = os.path.join(filepath_radar_files, radfile)
        if not os.path.isdir(filepath_radar_files):
            os.mkdir(filepath_radar_files)
        with open(file_path, "wb") as file:
            file.write(binary_data)
        print("done")

def make_timestring(start: str, end: str):
    """
    Turn datetime strings of start and end datetime into shortened datetime string

    Args:
        start (str): string of start datetime in format 'yyyy-mm-dd %H:%M:%S'
        end (str): string of end datetime in format 'yyyy-mm-dd %H:%M:%S'

    Returns:
        str: shortened string with both datetimes in format 'yymmddHHMM_yymmddHHMM'
    """

    short_start = dt.strptime(start, "%Y-%m-%d %H:%M:%S").strftime("%y%m%d%H%M")
    short_end = dt.strptime(end, "%Y-%m-%d %H:%M:%S").strftime("%y%m%d%H%M")
    return f"{short_start}_{short_end}"


def create_timeseries(start_datetime: str = MIN_DATE,
                      end_datetime: str = MAX_DATE,
                      radar_files: str = FILEPATH_RADAR):
    """
    read radar files from filepath and turn them into a csv outfile for each direction 
    (NE; E; SE; N; C; S; NW; W; SW) for the event.
    
    Args:
        start_datetime (str, optional): _description_. Defaults to MIN_DATE.
        end_datetime (str, optional): _description_. Defaults to MAX_DATE.
        radar_files (str, optional): _description_. Defaults to FILEPATH_RADAR.

    Returns:
        _type_: _description_
    """
    
    print("\n\ncreating timeseries for each direction")
    radarFiles = glob.glob(radar_files + r"\*.scu")
    data_dict = {key: [] for key in SENSORS_PIXELID}

    #data_dict_sum = {key: [] for key in SENSORS_PIXELID}

    data_dicts = {
        "NW": {key: [] for key in SENSORS_PIXELID},
        "W": {key: [] for key in SENSORS_PIXELID},
        "SW": {key: [] for key in SENSORS_PIXELID},
        "N": {key: [] for key in SENSORS_PIXELID},
        "C": {key: [] for key in SENSORS_PIXELID},
        "S": {key: [] for key in SENSORS_PIXELID},
        "NE": {key: [] for key in SENSORS_PIXELID},
        "E": {key: [] for key in SENSORS_PIXELID},
        "SE": {key: [] for key in SENSORS_PIXELID},
    }

    grid_keys = {
        "NW": -35,
        "W": -34,
        "SW": -33,
        "N": -1,
        "C": 0,
        "S": 1,
        "NE": 33,
        "E": 34,
        "SE": 35,
    }

    for file in radarFiles:

        filename = file.split("\\")[-1]
        file_datetime = dt.strptime(filename.split(".")[0][2:], "%y%m%d%H%M")

        if file_datetime < dt.strptime(start_datetime, "%Y-%m-%d %H:%M:%S"):
            print(f"{file_datetime} < {start_datetime}")
            continue

        elif file_datetime > dt.strptime(end_datetime, "%Y-%m-%d %H:%M:%S"):
            print(f"{file_datetime} > {end_datetime}")
            continue

        print(f"{'='*20}<{filename}>{'='*20}")
        print(filename, file_datetime)
        # Open NetCDF file from response content
        ncf = netCDF4.Dataset(file, mode="r")
        image_data = ncf.groups["dataset_DXk"].variables['image'][:]
        ncf.close()
        
        # Extract and process the specific array sections
        array_result = [image_data[i, 147:201] for i in range(119, 153)]
        transposed_arrays = list(map(list, zip(*array_result)))
        array_result = np.array(transposed_arrays)/12  ##Divide per 12 to give the result in mm
        
        # Flatten the nested list
        list_array = [item for sublist in array_result for item in sublist]

        # Extract values from list_array using indices
        extracted_values = {}
        for direction, grid_move in grid_keys.items():
            extracted_values[direction] = [list_array[i + grid_move] for i in SENSORS_PIXELID.values()]

        # Append the extracted values to the corresponding sensorsPixelId.keys() in the dictionary

        for i, key in enumerate(SENSORS_PIXELID.keys()):
            #print("\ncurrent key:", key)
            for direction, value in extracted_values.items():
                #print(f"current direction: {direction}")
                #print(f"appending to {data_dicts[direction][key]}")
                data_dicts[direction][key].append(value[i])

    time_intervals = pd.date_range(start=start_datetime, end=end_datetime, freq="5min")
    time_intervals = pd.DataFrame(columns=["time"], data=time_intervals)

    data_dfs = {}
    for direction, data_dict in data_dicts.items():
        data_dfs[direction] = pd.DataFrame(data_dict)
        data_dfs[direction]["time"] = time_intervals["time"]

        data_dfs[direction] = data_dfs[direction][['time','E1E54', 'E223A', 'E3C10', 'E434E', 'E436B', 'E45DB', 'E45EC', 'E45ED',
                                                    'E45F8', 'E4692', 'E46BE', 'E4A7A', 'E4AC1', 'E4DB5', 'E4E8B', 'E4EB2',
                                                    'E5217', 'E525C', 'E5268', 'E52A7', 'E53B4', 'E557C', 'E570D', 'E572D',
                                                    'E5744', 'E57C3', 'E57D3', 'E5B98', 'E5B9D', 'E5BA7', 'E5FC8', 'E603E',
                                                    'E6040', 'E6069', 'E611E', 'E6120', 'E6A24', 'E7052', 'E7217', 'E72F2',
                                                    'E73A3', 'E75A1', 'E761F', 'E7A34', 'E7B54', 'E7DE6', ]]

    outpath = FILEPATH_RADAR_DIR
    if not os.path.isdir(outpath):
        os.mkdir(outpath)
        
    timestring = make_timestring(start_datetime, end_datetime)
    
    for direction, data_df in data_dfs.items():
        file = f"{timestring}_{direction}.csv"
        filestring = os.path.join(outpath, file)
        
        data_df.to_csv(filestring, index=False)
    
    print("done")
    return data_dfs

# ============================== PLOTTING FUNCTIONS ===========================
def correlation_class(correlation):
   
    if correlation is None:
        return 0  # Black for undefined correlation
    elif -30 <= correlation < 0:
        return "No correlation" 
    elif 0 <= correlation < 30:
        return "No correlation"  
    elif 30 <= correlation < 50:
        return "Low"  
    elif 50 <= correlation < 70:
        return "Moderate"  
    elif 70 <= correlation <= 100:
        return "Strong"  #
    else:
        return "#000000"  # Black for out-of-range values
    

def correlation_to_color(correlation):
    """
    This function categorizes correlation values into different color ranges.
    Args:
        correlation (float or None): Correlation value between -1 and 1.

    Returns:
        str: Hex color code.
    """
    if correlation is None:
        return "#000000"  # Black for undefined correlation
    elif correlation < -70:
        return "#ff0101"  
    elif -70 <= correlation < -50:
        return "#FF9900" 
    elif -50 <= correlation < -30:
        return "#FFFF00" 
    elif -30 <= correlation < 0:
        return "#848482" 
    elif 0 <= correlation < 30:
        return "#848482"  
    elif 30 <= correlation < 50:
        return "#D5FFD0"  
    elif 50 <= correlation < 70:
        return "#40F8FF"  
    elif 70 <= correlation <= 100:
        return "#279EFF"  #
    else:
        return "#000000"  # Black for out-of-range values


if __name__ == "__main__":
    main()