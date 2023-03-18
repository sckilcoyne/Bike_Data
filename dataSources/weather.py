'''

https://pirateweather.net/

Google NOAA Datasets
    https://console.cloud.google.com/marketplace/browse?filter=partner:NOAA&project=python-232920
Amazon NOAA Datasets
    https://registry.opendata.aws/collab/noaa/


High Resolution Rapid Refresh Model (HRRR)
    https://rapidrefresh.noaa.gov/hrrr/

    The HRRR Zarr Archive Managed by MesoWest by Taylor Gowan
        https://mesowest.utah.edu/html/hrrr/

    HRRR-B Python package: download and read HRRR grib2 files by Brian Blaylock
        https://github.com/blaylockbk/Herbie

    Using Cloud Computing to Analyze Model Output Archived in Zarr Format
        https://journals.ametsoc.org/view/journals/atot/39/4/JTECH-D-21-0106.1.xml

    Variable Descriptions
        https://home.chpc.utah.edu/~u0553130/Brian_Blaylock/HRRR_archive/hrrr_sfc_table_f00-f01.html

    Python Data Loading
        https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/python_data_loading.html
'''
# %% Initialize
import dataclasses
import datetime
# import zarr
import s3fs
import xarray
import boto3
from botocore import UNSIGNED
from botocore.config import Config
# import cartopy.crs as ccrs
import numcodecs as ncd
import numpy as np


ZARR_BUCKET = 'hrrrzarr'

s3 = s3fs.S3FileSystem(anon=True)
def lookup(path):
    '''Lookup
    '''
    return s3fs.S3Map(path, s3=s3)

DATE = '20210101'

LEVEL_TYPE = 'sfc' # Surface
# LEVEL_TYPE = 'prs' # Pressure

LEVEL = 'surface'
# LEVEL = '2m_above_ground'

VARIABLE = 'TMP' # Temperature, K

CHUNK_ID = '5.10'

# PATH_HRRRZARR = f'{ZARR_BUCKET}/{LEVEL_TYPE}/{DATE}/{DATE}_00z_anl.zarr/{LEVEL}/{VARIABLE}/{LEVEL}/{VARIABLE}/{CHUNK_ID}'

print('Constants created')

# ds = xarray.open_mfdataset([lookup(PATH_HRRRZARR), lookup(f"{PATH_HRRRZARR}/surface")],
#                             engine="zarr")

# ds.TMP.plot()

'''Use Case 3: Gridpoint, multiple model runs
https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/python_data_loading.html#Use-Case-3:-Gridpoint,-multiple-model-runs
'''
@dataclasses.dataclass
class ZarrId:
    '''Zarr ID
    '''
    run_hour: datetime.datetime
    level_type: str
    var_level: str
    var_name: str
    model_type: str

    def format_chunk_id(self, chunk_id):
        '''Format Chunk ID
        '''
        if self.model_type == "fcst":
            # Extra id part since forecasts have an additional (time) dimension
            return "0." + str(chunk_id)
        else:
            return chunk_id

print('ZarrId class created')

#%% Generate URLs - cells 2 and 3
print('\n\nGenerate URLs')
# Cell 2
level_type = "sfc"
model_type = "fcst"
run_hour = datetime.datetime(2021, 1, 1, 7)

def create_hrrr_zarr_explorer_url(level_type, model_type, run_hour):
    url = "https://hrrrzarr.s3.amazonaws.com/index.html"
    url += run_hour.strftime(
        f"#{level_type}/%Y%m%d/%Y%m%d_%Hz_{model_type}.zarr/")
    return url

hrrr_zarr_explorer_url = create_hrrr_zarr_explorer_url(level_type, model_type, run_hour)
print(f'{hrrr_zarr_explorer_url=}')
hrrr_zarr_explorer_url = create_hrrr_zarr_explorer_url("prs", "anl", run_hour)
print(f'{hrrr_zarr_explorer_url=}')

# Cell 3
zarr_id = ZarrId(
                run_hour=datetime.datetime(2020, 8, 1, 0), # Aug 1, 0Z
                level_type="sfc",
                var_level="1000mb",
                var_name="TMP",
                model_type="anl"
                )
chunk_id = "4.3"

def create_https_chunk_url(zarr_id, chunk_id):
    url = "https://hrrrzarr.s3.amazonaws.com"
    url += zarr_id.run_hour.strftime(
        f"/{zarr_id.level_type}/%Y%m%d/%Y%m%d_%Hz_{zarr_id.model_type}.zarr/")
    url += f"{zarr_id.var_level}/{zarr_id.var_name}/{zarr_id.var_level}/{zarr_id.var_name}"
    url += f"/{zarr_id.format_chunk_id(chunk_id)}"
    return url

chunk_url = create_https_chunk_url(zarr_id, chunk_id)
print(f'{chunk_url=}')

# %% For use in code - cells 4-6
print('\n\nFor use in code')
# Cell 4
def create_s3_group_url(zarr_id, prefix=True):
    url = "s3://hrrrzarr/" if prefix else "" # Skip when using boto3
    url += zarr_id.run_hour.strftime(
        f"{zarr_id.level_type}/%Y%m%d/%Y%m%d_%Hz_{zarr_id.model_type}.zarr/")
    url += f"{zarr_id.var_level}/{zarr_id.var_name}"
    return url

group_url = create_s3_group_url(zarr_id)
print(f'{group_url=}')

# Cell 5
def create_s3_subgroup_url(zarr_id, prefix=True):
    url = create_s3_group_url(zarr_id, prefix)
    url += f"/{zarr_id.var_level}"
    return url

subgroup_url = create_s3_subgroup_url(zarr_id)
print(f'{subgroup_url=}')

# Cell 6
def create_s3_chunk_url(zarr_id, chunk_id, prefix=False):
    url = create_s3_subgroup_url(zarr_id, prefix)
    url += f"/{zarr_id.var_name}/{zarr_id.format_chunk_id(chunk_id)}"
    return url

chunk_url = create_s3_chunk_url(zarr_id, chunk_id)
print(f'{chunk_url=}')

#%% Handling the Projection - cell 7
# projection = ccrs.LambertConformal(central_longitude=262.5, 
#                                    central_latitude=38.5, 
#                                    standard_parallels=(38.5, 38.5),
#                                     globe=ccrs.Globe(semimajor_axis=6371229,
#                                                      semiminor_axis=6371229))
#%% Loading the data
# Use Case 1: Whole grid, one model run
# Use Case 2: Whole grid, multiple model runs
# Use Case 3: Gridpoint, multiple model runs
# Use Case 4: Small region of grid, multiple model runs

#%% Use Case 1: Whole grid, one model run - cell 8
print('Use Case 1')
# def load_dataset(urls):
#     fs = s3fs.S3FileSystem(anon=True)    
#     ds = xr.open_mfdataset([s3fs.S3Map(url, s3=fs) for url in urls], engine='zarr')
    
#     # add the projection data
#     ds = ds.rename(projection_x_coordinate="x", projection_y_coordinate="y")
#     ds = ds.metpy.assign_crs(projection.to_cf())
#     ds = ds.metpy.assign_latitude_longitude()    
#     return ds

# dataset = load_dataset([create_s3_group_url(zarr_id), create_s3_subgroup_url(zarr_id)])


#%% Use Case 2: Whole grid, multiple model runs - cells 9-10
print('Use Case 2')

# Cell 9
fs = s3fs.S3FileSystem(anon=True)
print('S3 filesystem created')

def load(url, run_hour=None, new_time_dimension=None):
    # Download the data from S3. May be lazy.
    ds = xr.open_dataset(s3fs.S3Map(url, s3=fs), engine="zarr")
    
    # Add the model run hour in as a dimension
    if run_hour is not None:
        ds[new_time_dimension] = run_hour
        ds = ds.set_coords(new_time_dimension)
    
    # Later on we use metpy functions that expect the grid variables to be x and y
    ds = ds.rename(projection_x_coordinate="x", projection_y_coordinate="y")
    return ds

# def load_combined_dataset(zarr_ids):
    
#     # Get the grid data (at a long forecast hour in case the whole time dim is needed)
#     grid_zarr_id = dataclasses.replace(zarr_ids[0]) # dataclasses.replace is just a fancy copy function
#     grid_zarr_id.run_hour = grid_zarr_id.run_hour.replace(hour=0)  
#     grid = load(create_s3_group_url(grid_zarr_id))
    
#     is_forecast = zarr_ids[0].model_type == "fcst"
#     new_time_dimension = "reference_time" if is_forecast else "time"
    
#     datasets = [load(create_s3_subgroup_url(zarr_id), zarr_id.run_hour, new_time_dimension) 
#                 for zarr_id in zarr_ids]
    
#     if is_forecast: # Align the time axes of each dataset (b/c forecasts have different lengths)
#         for dataset in datasets:
#             dataset["time"] = grid["time"][:len(dataset["time"])]
#         datasets = xr.align(*datasets, join="outer")
        
#     ds = xr.concat(datasets, dim=new_time_dimension, combine_attrs="override")
    
#     # Add the geospatial data to the combined dataset
#     ds["x"] = grid["x"]
#     ds["y"] = grid["y"]  
#     ds = ds.metpy.assign_crs(projection.to_cf())
#     ds = ds.metpy.assign_latitude_longitude()
    
#     return ds

# Cell 10
zarr_ids = [ZarrId(
                run_hour=datetime.datetime(2021, 7, 16, 6) + datetime.timedelta(hours=time_delta),
                level_type="sfc",
                var_level="entire_atmosphere_single_layer",
                var_name="PWAT",
                model_type="fcst"
                )
            for time_delta in range(3)] # get 3 hours starting at the given time

# load_combined_dataset(zarr_ids)

#%% Use Case 3: Gridpoint, multiple model runs
print('Use Case 3')
# Cell 11
# chunk_index = xarray.open_zarr(s3fs.S3Map(f's3://{ZARR_BUCKET}/grid/HRRR_chunk_index.zarr', s3=fs))
# print(f'{chunk_index=}')

# Cell 12
# def get_nearest_point(projection, chunk_index, longitude, latitude):
#     x, y = projection.transform_point(longitude, latitude, ccrs.PlateCarree())
#     return chunk_index.sel(x=x, y=y, method="nearest")

# nearest_point = get_nearest_point(projection, chunk_index, -111.8910, 40.7608)
# chunk_id = nearest_point.chunk_id.values

chunk_id = '5.10'
print(f'{chunk_id=}')

# Cell 13
# Don't recreate this resource in a loop! That caused a 3-4x slowdown for me.
s3 = boto3.resource(service_name='s3', region_name='us-west-1', config=Config(signature_version=UNSIGNED))

def retrieve_object(s3, s3_url):
    obj = s3.Object('hrrrzarr', s3_url)
    return obj.get()['Body'].read()

zarr_id = ZarrId(
                run_hour=datetime.datetime(2019, 1, 1, 12),
                level_type="sfc",
                var_level="2m_above_ground",
                var_name="TMP",
                model_type="anl"
                )

compressed_data = retrieve_object(s3, create_s3_chunk_url(zarr_id, chunk_id))

# Cell 14
def decompress_chunk(zarr_id, compressed_data):
    buffer = ncd.blosc.decompress(compressed_data)
    
    dtype = "<f2"
    if zarr_id.var_level == "surface" and zarr_id.var_name == "PRES":
        dtype = "<f4"
        
    chunk = np.frombuffer(buffer, dtype=dtype)
    
    if zarr_id.model_type == "anl":
        data_array = np.reshape(chunk, (150, 150))
    else:
        entry_size = 22500
        data_array = np.reshape(chunk, (len(chunk)//entry_size, 150, 150))
        
    return data_array

chunk_data = decompress_chunk(zarr_id, compressed_data)

# Cell 15
chunk_data[nearest_point.in_chunk_y.values, nearest_point.in_chunk_x.values]


# Cell 16

# Cell 17

#%% Use Case 4: Small region of grid, multiple model runs - cells 11-17
print('Use Case 4')


if __name__ == '__main__':
    print('WEATHER')
