"""Reading National Hurricane Center Data.

======================================
By: Aodhan Sweeney
This program is written to pull data from the National Hurricane
Center and return the data in an easy to use format.

"""

from datetime import datetime

import numpy as np
import pandas as pd
import requests


class NHCD():
    """
    Read data from the National Hurricane Center Database (NHCD).

    This class reads and then makes dataframes to easier access NHC Data.

    """

    def __init__(self):
        """
        Create with member attributes and storm info.

        This initiation creates a file table based on a url for all storms in the
        NHCD and puts them into a pandas dataframe. This dataframe is then turned
        into a member atribute '.storm_table'.

        """
        storm_list_columns = ['Name', 'Basin', 'CycloneNum', 'Year', 'StormType', 'Filename']
        file_table = pd.read_csv('http://ftp.nhc.noaa.gov/atcf/index/storm_list.txt',
                                 names=storm_list_columns, header=None, index_col=False,
                                 usecols=[0, 1, 7, 8, 9, 20])
        file_table.Filename = file_table.Filename.str.lower()
        self.storm_table = file_table

    def get_tracks(self, year, filename):
        """
        Make url and pulls track data for a given storm.

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
            in the NHCD. The first character is defaulted as space in NHCD so it is clipped
            when being used.

        Returns
        -------
        unique_models: list
            all the models that have run forecasts for this storm throughout its life

        """
        today = datetime.today()
        current_year = today.year
        data_dictionary = {}
        # Current year data is stored in a different location
        if year == str(current_year):
            unformatted_forecast_url = 'http://ftp.nhc.noaa.gov/atcf/aid_public/a{}.dat.gz'
            urlf = unformatted_forecast_url.format(filename[1:])
            unformatted_best_url = 'http://ftp.nhc.noaa.gov/atcf/btk/b{}.dat'
            urlb = unformatted_best_url.format(filename[1:])
        else:
            unformatted_forecast_url = 'http://ftp.nhc.noaa.gov/atcf/archive/{}/a{}.dat.gz'
            urlf = unformatted_forecast_url.format(year, filename[1:])
            unformatted_best_url = 'http://ftp.nhc.noaa.gov/atcf/archive/{}/b{}.dat.gz'
            urlb = unformatted_best_url.format(year, filename[1:])

        url_links = [urlf, urlb]
        url_count = 0
        for url in url_links:
            # Checking if url is valid, if status_code is 200 then website is active
            if requests.get(url).status_code == 200:
                # Creating column names
                storm_data_column_names = ['Basin', 'CycloneNum', 'WarnDT', 'Model',
                                           'Forecast_hour', 'Lat', 'Lon']
                # Create a pandas dataframe using specific columns for a storm
                single_storm = pd.read_csv(url, header=None, names=storm_data_column_names,
                                           index_col=False, usecols=[0, 1, 2, 4, 5, 6, 7])

                # Must convert lats and lons from string to float and preform division by 10
                storm_lats = single_storm['Lat']
                storm_lats = (storm_lats.str.slice(stop=-1))
                storm_lats = storm_lats.astype(float)
                storm_lats = storm_lats / 10
                single_storm['Lat'] = storm_lats

                storm_lons = single_storm['Lon']
                storm_lons = (storm_lons.str.slice(stop=-1))
                storm_lons = storm_lons.astype(float)
                storm_lons = -storm_lons / 10
                single_storm['Lon'] = storm_lons

                # Change WarnDT to a string
                single_storm['WarnDT'] = [str(x) for x in single_storm['WarnDT']]

                # Adding this newly created DataFrame to a dictionary
                if url_count == 0:
                    data_dictionary['forecast'] = single_storm
                else:
                    data_dictionary['best_track'] = single_storm

            else:
                raise('url {} was not valid, select different storm.'.format(url))

            url_count += 1
        # Turn data_dictionary into a member attribute
        self.storm_dictionary = data_dictionary
        forecast = data_dictionary.get('forecast')
        unique_models, unique_index = list(np.unique(forecast['Model'].values,
                                           return_index=True))
        return(unique_models)

    def model_selection_latlon(self, models):
        """
        Select model type and get lat/lons and track evolution data.

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
