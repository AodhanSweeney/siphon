"""Reading Storm Prediction Center Data.

======================================
By: Aodhan Sweeney
This program pulls data from the Storm Prediction
Center's Archive of data that goes back to the 1950s.
Weather events that are available are
hail, wind, and tornados.

"""

import pandas as pd


class SpcData:
    """
    Pulls data from the SPC center.

    This class gets data on tornados, hail, and severe wind events.
    This class will return a pandas dataframe for each of these storm events.

    """

    def __init__(self, stormtype, datetime):
        """
        Create class of SpcData with attributes that select date, and storm type.

        SPC data sifting method is differentiated based on whether the storm is before 2018
        or not. Storms after 12/31/2017 will be found by using the specific URL for the date
        selected and finding the csv file on the SPC website. Storms prior to this date are
        first collected into a large pd dataframe with all SPC events of a selected type
        from 1955-2017. This data is then successively parced and trim until storms from one
        date are achieved. This is because the SPC has changed the way they display data so
        many times that to write specific methods for each time frame would be too time
        consuming.

        """
        self.storm_type = stormtype
        self.date_time = datetime
        self.year_string = self.date_time[0:4]
        self.month_string = self.date_time[4:6]
        self.day_string = self.date_time[6:8]
        self.storms = self.storm_type_selection()

        if int(self.year_string) < 2017:
            one_year_table = self.storms[self.storms['Year'] == int(self.year_string)]

            month_table = one_year_table[one_year_table['Month'] == int(self.month_string)]
            self.day_table = month_table[month_table['Day'] == int(self.day_string)]

        elif int(self.year_string) >= 2017:
            self.day_table = self.storms

    def storm_type_selection(self):
        """
        Find and create the url for a specific storm type and year.

        Prior to 2017, the ways in which the SPC storm data is inconsistent. In order
        to deal with this, the Urls used to find the data for a given day changes
        based on the year chosen by the user.

        Parameters
        ----------
        self:
            The date_time string attribute will be used for year identification

        Returns
        -------
        (torn/wind/hail)_reports: pandas DataFrame
            This dataframe has the data about the specific SPC data type for either one day
            or a 60+ year period based on what year is chosen.

        """
        # Place holder string 'mag' will be replaced by event type (tornado, hail or wind)
        mag = str
        # Colums are different for events before and after 12/31/2017.
        after_2017_columns = ['Time', mag, 'Location', 'County', 'State',
                              'Lat', 'Lon', 'Comment']
        before_2018_columns = ['Num', 'Year', 'Month', 'Day', 'Time', 'Time Zone',
        'State', mag, 'Injuries', 'Fatalities', 'Property Loss', 'Crop Loss', 'Start Lat',
        'Start Lon', 'End Lat', 'End Lon', 'Length (mi)', 'Width (yd)', 'Ns', 'SN', 'SG',
        'County Code 1', 'County Code 2', 'County Code 3', 'County Code 4']

        # Find specific urls and create dataframe based on time and event type
        if self.storm_type == 'tornado':
            if int(self.year_string) <= 2017:
                before_2018_columns[7] = 'F-Scale'
                url = 'https://www.spc.noaa.gov/wcm/data/1950-2017_torn.csv'
                torn_reports = pd.read_csv(url, names=before_2018_columns, header=0,
                index_col=False, usecols=[0, 1, 2, 3, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27])
            else:
                _url = 'https://www.spc.noaa.gov/climo/reports/{}{}{}_rpts_filtered_torn.csv'
                url = _url.format(self.year_string[2: 4], self.month_string, self.day_string)
                after_2017_columns[1] = 'F-Scale'
                torn_reports = pd.read_csv(url, names=after_2017_columns,
                header=0, index_col=False, usecols=[0, 1, 2, 3, 4, 5, 6, 7])
            return(torn_reports)

        elif self.storm_type == 'hail':
            if int(self.year_string) <= 2017:
                before_2018_columns[7] = 'Size (hundredth in)'
                url = 'https://www.spc.noaa.gov/wcm/data/1955-2017_hail.csv'
                hail_reports = pd.read_csv(url, names=before_2018_columns, header=0,
                index_col=False, usecols=[0, 1, 2, 3, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27])

            else:
                _url = 'https://www.spc.noaa.gov/climo/reports/{}{}{}_rpts_filtered_hail.csv'
                url = _url.format(self.year_string[2:4], self.month_string, self.day_string)
                after_2017_columns[1] = 'Size (in)'
                hail_reports = pd.read_csv(url, names=after_2017_columns,
                header=0, index_col=False, usecols=[0, 1, 2, 3, 4, 5, 6, 7])
            return(hail_reports)

        elif self.storm_type == 'wind':
            if int(self.year_string) <= 2017:
                before_2018_columns[7] = 'Speed (kt)'
                url = 'https://www.spc.noaa.gov/wcm/data/1955-2017_wind.csv'
                wind_reports = pd.read_csv(url, names=before_2018_columns, header=0,
                index_col=False, usecols=[0, 1, 2, 3, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27])
            else:
                _url = 'https://www.spc.noaa.gov/climo/reports/{}{}{}_rpts_filtered_wind.csv'
                url = _url.format(self.year_string[2:4], self.month_string, self.day_string)
                after_2017_columns[1] = 'Speed (kt)'
                wind_reports = pd.read_csv(url, names=after_2017_columns, header=0,
                index_col=False, usecols=[0, 1, 2, 3, 4, 5, 6, 7])
            return(wind_reports)

        else:
            raise('Not a valid event type: enter either tornado, wind or hail.')
