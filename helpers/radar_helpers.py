import os 
from datetime import datetime as dt, timedelta as td
import requests
import json
import glob
import numpy as np
import pandas as pd
import geopandas as gpd




def get_radar_files(start_datetime,
                    end_datetime,
                    filepath_radar_files):
    
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
        
def get_grid_coords(grid_file):
    """get latitude and longitude grid for current grid

    Returns:
        lat_values list[int]: all latitude values for current grid
        lon_values list[int]: all longitude values for current grid
    """
    
    # REWORK FOR PRODUCTION 
    
    #Reading the radar grid of the study area
    grid_coord=gpd.read_file(grid_file)
    #Converting to crs UTM EPSG:25832
    grid_coord=grid_coord.to_crs("EPSG:25832")
    #Extracting the lon max, lon min, lat max and lat min for the first and last pixel
    lon_min, lat_max = round(grid_coord.centroid[0].x), round(grid_coord.centroid[0].y)
    lon_max, lat_min = round(grid_coord.centroid[len(grid_coord)-1].x), round(grid_coord.centroid[len(grid_coord)-1].y)
    #Creating a list to store the longitudes from each pixel
    lon_values=[]
    for i in range(lon_min,lon_max+1000,1000):   #Take into account that the distance Between 2 centroid is 1000 meters
        lon_values.append(i)

    lat_values = []
    for i in range(lat_min, lat_max +1000, 1000):
        lat_values.append(i)
    
    return lat_values, lon_values 
    

def create_timeseries(start_datetime,
                      end_datetime,
                      radar_files):
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
        data_dfs[direction]["time"] = 0["time"]

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