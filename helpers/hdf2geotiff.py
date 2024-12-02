from osgeo import gdal

# # infile = 'e:/workE/hd2107140715.scu'
# infile = 'E:/workE/MSG2-SEVI-MSG15-0100-NA-20190626222918.877000000Z-NA.nat'
# #infile = 'd:/projekt/hm340_LAWA/hd2107140715.cum'

# outfile = 'MSG2-SEVI-MSG15.tiff'
# # outfile = 'scu2107140715.tiff'
# #outfile = 'cum2107140715.tiff'

# kwargs = {
#     'format': 'GTiff',
#     'noData' : -999, # bei Einzelbildern
#     #'noData' : -1, # bei Cumbildern
#     # Projektion
#     'outputSRS' : '+proj=stere +lat_0=90 +lat_ts=60 +lon_0=10 +x_0=0 +y_0=0 +R=6370040 +units=m +no_defs', 
#     'outputBounds' : [-523462.0000000000000000, -3758645.0000000000000000, 376538.0000000000000000, -4658645.0000000000000000]
# }


infile = 'sd3_20230302104011.hdf5'
outfile = 'MSG2-SEVI-MSG15.tiff'

kwargs = {
    'format': 'GTiff',
    'noData' : -999, # bei Einzelbildern
    #'noData' : -1, # bei Cumbildern
    # Projektion
    'outputSRS' : '+proj=geos +lat_0=0 +lon_0=9.5 +x_0=0 +y_0=0 +h=35785831.0 +no_defs +a=6378169 +rf=295.488065897014 +units=m', 
    'outputBounds' : [5567248.0742, 5570248.4773, -5570248.4773, 1393687.2705]    
}

# Datei öffnen
in_ds = gdal.OpenEx(infile)
# in GeoTiff uebersetzen und Angaben zu Projektion und noData eintragen
out_ds = gdal.Translate(outfile, in_ds, **kwargs)
