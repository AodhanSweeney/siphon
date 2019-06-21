
""" Reading National Hurricane Center Data
======================================
By: Aodhan Sweeney

This program is written to pull data from the National Hurricane
Center and return the data in an easy to use format.
"""
# Setting things up...
import gzip
from io import BytesIO
from io import StringIO

import numpy as np
from pandas import DataFrame
import requests

def read_urlfile(url):
    """read_urlfile is a function created to read a .dat file from a given url
    and compile it into a list of strings with given headers.

    Parameters
    ----------
    url: string
        location of the NHC database

    Returns
    -------
    data: list
        parced data of NHC in list format
    """
    headers = {'User-agent': 'Unidata Python Client Test'}
    response = requests.get(url, headers=headers)
    # Store data response in a string buffer
    string_buffer = StringIO(response.text)
    # Read from the string buffer as if it were a physical file
    data = string_buffer.getvalue()
    return data.splitlines()


def read_gzfile(url):
    """readGZFile is a function which opens and reads zipped files. In this case it takes in a
    .gzfile containing information on each storm and returns a byte buffer split based on
    lines.

    Parameters
    ----------
    url: string
        location of the NHC database

    Returns
    -------
    data: list
        parced data of NHC in list format
    """

    headers = {'User-agent': 'Unidata Python Client Test'}
    response = requests.get(url, headers=headers)
    # Store data response in a bytes buffer
    bio_buffer = BytesIO(response.content)
    # Read from the string buffer as if it were a physical file
    gzf = gzip.GzipFile(fileobj=bio_buffer)
    data = gzf.read()
    return data.splitlines()


def split_storm_info(storm_list):
    """split_storm_info takes a list of strings and creates a pandas dataframe
    for the data set taken off the NHC archive. This function is called in the main to
    find all storms.

    Parameters
    ----------
    storm_list: list
        parced list of NHC data in list format

    Returns
    -------
    storms: pandas.DataFrame
        dataframe of NHC info
    """
    name, cyclonenum, year, stormtype, basin, filename = [], [], [], [], [], []
    for line in storm_list[1:]:
        fields = line.split(',')
        name.append(fields[0].strip())
        basin.append(fields[1].strip())
        cyclonenum.append(fields[7].strip())
        year.append(fields[8].strip())
        stormtype.append(fields[9].strip())
        filename.append(fields[-1].strip().lower())

    storms = DataFrame({'Name': name, 'Basin': basin, 'CycloneNum': np.array(cyclonenum),
                        'Year': np.array(year), 'StormType': stormtype,
                        'Filename': filename})
    return(storms)


class NHCD():
    """This class is made to read and write data from the National
    Hurricane Center Database (NHCD)."""
    def __init__(self):
        # Setting up storm object table
        fileLines = read_urlfile('http://ftp.nhc.noaa.gov/atcf/index/storm_list.txt')
        self.storm_table = split_storm_info(fileLines)


    def get_tracks(self, year, filename):
        """get_tracks is a function that will create the url and pull the data for either
        the forecast track or best track for a given storm. The Url is made by using both
        the year and the filename. This function will then read the data and create a data
        frame for both the forecast and best tracks and compile these data frames into a
        dictionary. This function returns this dictionary of forecast and best track.

        Parameters
        ----------
        self:
            storing the storm dictionary as member attrubute of NHCD
        year: int
            year of the storm incident
        filename: str
            unique filename of the storm which is used for indexing purposes and id
            in the NHCD

        Returns
        -------
        unique_models: list
            all the models that have run forecasts for this storm throughout its life
        """
        year = str(year)
        data_dictionary = {}
        # Current year data is stored in a different location
        if year == '2019':
            urlf = 'http://ftp.nhc.noaa.gov/atcf/aid_public/a{}.dat.gz'.format(filename)
            urlb = 'http://ftp.nhc.noaa.gov/atcf/btk/b{}.dat'.format(filename)
        else:
            urlf = 'http://ftp.nhc.noaa.gov/atcf/archive/{}/a{}.dat.gz'.format(year, filename)
            urlb = 'http://ftp.nhc.noaa.gov/atcf/archive/{}/b{}.dat.gz'.format(year, filename)

        url_links = [urlf, urlb]
        url_count = 0
        for url in url_links:
            # Checking if url is valid, if status_code is 200 then website is active
            if requests.get(url).status_code == 200:
                if url.endswith('.dat'):
                    lines = read_urlfile(url)
                else:
                    lines = read_gzfile(url)

                # Splitting the method for which we will create the dataframe
                lat, lon, basin, cyclonenum = [], [], [], []
                warn_dt, model, forecast_hour = [], [], []
                for line in lines:
                    line = str(line)
                    line = line[2:]
                    fields = line.split(',')
                    # Joins together lattitude and longitude strings without
                    # directional letters.
                    # Includes type conversion in order to divide by 10 to
                    # get the correct coordinate.
                    latsingle = int(fields[6][:-1])/10.0
                    lonsingle = -(int(fields[7][:-1])/10.0)
                    lat.append(latsingle)
                    lon.append(lonsingle)
                    basin.append(fields[0])
                    forecast_hour.append(fields[5])
                    cyclonenum.append(fields[1].strip())
                    warn_dt.append(fields[2].strip())
                    model.append(fields[4].strip())

                    # Combining data from file into a Pandas Dataframe.
                    storm_data_frame = DataFrame({'Basin': basin,
                                                  'CycloneNum': np.array(cyclonenum),
                                                  'WarnDT': np.array(warn_dt),
                                                  'Model': model, 'Lat': np.array(lat),
                                                  'Lon': np.array(lon),
                                                  'forecast_hour':
                                                  np.array(forecast_hour)})
                    # Adding this newly created DataFrame to a dictionary
                    if url_count == 0:
                        data_dictionary['forecast'] = storm_data_frame
                    else:
                        data_dictionary['best_track'] = storm_data_frame

            else:
                print('url {} was not valid, select different storm.'.format(url))

            url_count += 1

        self.storm_dictionary = data_dictionary
        forecast = data_dictionary.get('forecast')
        unique_models, unique_index = list(np.unique(forecast['Model'].values,
                                           return_index=True))
        return(unique_models)

    def model_selection_latlon(self, models):
        """model_selection_latlon is a function that allows the user to select a model for
        a given storm and whether the tracks are forecast or best tracks. The parameters
        for this are a string stating whether the user wants forecast or best tracks and
        also all model outputs for all forecasts and best tracks compiled into a python
        dictionary. The latlon part of this function comes from taking the users selected
        model and getting the latitudes and longitudes of all positions of the storm for
        this forecast. This function then returns these lats and lons as a pandas.Series

        Parameters
        ----------
        self:
            using storm dictionary attribute and also storing other model_table attribute
            and date_times attribute
        models: list
            unique models that are ran for a storm

        Returns
        -------
        self.model_table: list attribute
            all model forecasts for that specific model type that have been run for a given
            storm
        """
        # We will always plot best track, and thus must save the coordinates for plotting
        best_track = self.storm_dictionary.get('best_track')
        self.date_times = best_track['WarnDT']
        lats = best_track['Lat']
        lons = best_track['Lon']
        self.best_track_coordinates = [lats, lons]

        model_tracks = self.storm_dictionary.get('forecast')

        self.model_table = []
        for model in models:
            one_model_table = model_tracks[model_tracks['Model'] == model]
            self.model_table.append(one_model_table)

        return self.model_table
