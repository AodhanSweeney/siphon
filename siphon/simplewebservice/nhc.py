"""Reading National Hurricane Center Data.

======================================
By: Aodhan Sweeney
This program is written to pull data from the National Hurricane
Center and return the data in an easy to use format.
"""
import gzip
from io import BytesIO
from io import StringIO

import numpy as np
from pandas import DataFrame
import requests


def read_urlfile(url):
    """
    read_urlfile is a function created to read a .dat file from a given url.

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

    """
    The readGZFile is a function which opens and reads zipped files.

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
    """
    The split_storm_info takes a list of strings and creates a pandas dataframe for NHC data.

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
    """
    Reads data from the National Hurricane Center Database (NHCD).

    This class reads and then makes dataframes to easier access NHC Data.
    """
    def __init__(self):
        """
        Initiates the NHCD class with member attributes and storm info.

        This initiation creates a file lines list from a given url with all storms,
        and also a storm_table member attribute.
        """

        file_lines = read_urlfile('http://ftp.nhc.noaa.gov/atcf/index/storm_list.txt')
        self.storm_table = split_storm_info(file_lines)

    def get_tracks(self, year, filename):
        """
        Makes url and pulls track data for a given storm.

        The Url is made by using both the year and the filename. This function will then
        read the data and create a data frame for both the forecast and best tracks and
        compile these data frames into a dictionary. This function returns this dictionary
        of forecast and best track.

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
                    latsingle = int(fields[6][:-1]) / 10.0
                    lonsingle = -(int(fields[7][:-1]) / 10.0)
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
                raise('url {} was not valid, select different storm.'.format(url))

            url_count += 1

        self.storm_dictionary = data_dictionary
        forecast = data_dictionary.get('forecast')
        unique_models, unique_index = list(np.unique(forecast['Model'].values,
                                           return_index=True))
        return(unique_models)

    def model_selection_latlon(self, models):
        """
        Allows for model and storm selection and get lat/lons and track evolution data.

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
