"""Reading Storm Prediction Center Data.

======================================
By: Aodhan Sweeney
This program pulls data from the Storm Prediction
Center's Archive of data that goes back to the 1950s.
Weather events that are available are
hail, wind, and tornados.

"""

from io import StringIO

from pandas import DataFrame
import requests


def readurlfile(url):
    """
    Read a .dat file from a given url.

    Parameters
    ----------
    url: string
        location of the SPC reports for a specific given day

    Returns
    -------
    data: list
        parced data of SPC report in list format

    """
    headers = {'User-agent': 'Unidata Python Client Test'}
    response = requests.get(url, headers=headers)
    string_buffer = StringIO(response.text)
    data = string_buffer.getvalue()
    return data.splitlines()


class SpcData:
    """
    Pulls data from the SPC center.

    This class gets data on tornados, hail, and severe wind events. This class will return a
    pandas dataframe for each of these storm events.

    """

    def __init__(self, stormtype, datetime):
        """
        Create class of SpcData, initialized with stormtype and datetime for parsing.

        Parameters
        ----------
        stormtype: string
            either 'tornado', 'hail', or 'wind'
        datetime: string
            four digets for year (1955-2019), two for month, and two for day. Eg. 20170504
            meaning May 4th, 2017.

        """
        self.storm_type = stormtype
        self.date_time = datetime
        self.year_string = self.date_time[0:4]
        self.month_string = self.date_time[4:6]
        self.day_string = self.date_time[6:8]
        self.storms = self.storm_type_selection()
        if int(self.year_string) < 2017:
            one_year_table = self.storms[self.storms['Year'] == self.year_string]
            one_month_table = one_year_table[one_year_table['Month'] == self.month_string]
            self.one_day_table = one_month_table[one_month_table['Day'] == self.day_string]

        elif int(self.year_string) >= 2017:
            self.one_day_table = self.storms

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
            This dataframe has the data about the specific storm choice for either one day
            or a 60+ year period based on what year is chosen.

        """
        if self.storm_type == 'tornado':
            if int(self.year_string) <= 2017:
                url = 'https://www.spc.noaa.gov/wcm/data/1950-2017_torn.csv'
            else:
                zurl = 'https://www.spc.noaa.gov/climo/reports/{}{}{}_rpts_filtered_torn.csv'
                url = zurl.format(self.year_string[2: 4], self.month_string, self.day_string)
            torn_filelines = readurlfile(url)
            torn_reports = self.split_storm_info(torn_filelines, 'F-Scale')
            return(torn_reports)

        elif self.storm_type == 'hail':
            if int(self.year_string) <= 2017:
                url = 'https://www.spc.noaa.gov/wcm/data/1955-2017_hail.csv'
            else:
                zurl = 'https://www.spc.noaa.gov/climo/reports/{}{}{}_rpts_filtered_hail.csv'
                url = zurl.format(self.year_string[2:4], self.month_string, self.day_string)
            hail_filelines = readurlfile(url)
            hail_reports = self.split_storm_info(hail_filelines, 'Size (in)')
            return(hail_reports)

        elif self.storm_type == 'wind':
            if int(self.year_string) <= 2017:
                url = 'https://www.spc.noaa.gov/wcm/data/1955-2017_wind.csv'
            else:
                zurl = 'https://www.spc.noaa.gov/climo/reports/{}{}{}_rpts_filtered_wind.csv'
                url = zurl.format(self.year_string[2:4], self.month_string, self.day_string)
            wind_filelines = readurlfile(url)
            wind_reports = self.split_storm_info(wind_filelines, 'Speed (kt)')
            return(wind_reports)

    def split_storm_info(self, storm_list, mag_string):
        """
        Split storm information kept in a csv file provided by the SPC.

        Parameters
        ----------
        storm_list: list
            All storm events for either tornado (1950-2017),
            hail (1955-2017) or wind (1955-2017)
        mag_string: string
            Word that tells us what type of data is being stored in the
            'mag' section of the csv file. For tornados it is F-scale
            data, for wind it is speed, and for hail it is hail size.

        Returns
        -------
        storms: pandas DataFrame
            Data for the 60+ year time frame consolidated into one
            single dataframe

        """
        if int(self.year_string) <= 2017:
            om, year, mo, day, time, tz, st, sn = [], [], [], [], [], [], [], []
            stn, mag, inj, fat, loss, closs, slat = [], [], [], [], [], [], []
            slon, elat, elon, length, wid, ns, sg = [], [], [], [], [], [], []
            f1, f2, f3, f4 = [], [], [], []
            for line in storm_list[1:]:
                fields = line.split(',')
                om.append(fields[0].strip())
                year.append(fields[1].strip())
                mo.append(fields[2].strip())
                day.append(fields[3].strip())
                time.append(fields[5].strip())
                tz.append(fields[6].strip())
                st.append(fields[7].strip())
                stn.append(fields[9].strip())
                mag.append(fields[10].strip())
                inj.append(fields[11].strip())
                fat.append(fields[12].strip())
                loss.append(fields[13].strip())
                closs.append(fields[14].strip())
                slat.append(fields[15].strip())
                slon.append(fields[16].strip())
                elat.append(fields[17].strip())
                elon.append(fields[18].strip())
                length.append(fields[19].strip())
                wid.append(fields[20].strip())
                ns.append(fields[21].strip())
                sn.append(fields[22].strip())
                sg.append(fields[23].strip())
                f1.append(fields[24].strip())
                f2.append(fields[25].strip())
                f3.append(fields[26].strip())
                f4.append(fields[27].strip())

            storms = DataFrame({'Num': om, 'Year': year, 'Month': mo, 'Day': day,
                                'Time': time, 'Time Zone': tz, 'State': st, mag_string: mag,
                                'Injuries': inj, 'Fatalities': fat, 'Property Loss': loss,
                                'Crop loss': closs, 'Start lat': slat, 'Start lon': slon,
                                'End lat': elat, 'End lon': elon, 'Length (mi)': length,
                                'Width (yrd)': wid, 'NS': ns, 'SN': sn, 'SG': sg,
                                'County Code 1': f1, 'County Code 2': f2, 'County Code 3': f3,
                                'County Code 4': f4})
            return(storms)

        else:
            time, mag, location, county = [], [], [], []
            state, lat, lon, comment = [], [], [], []
            for line in storm_list[1:]:
                fields = line.split(',')
                time.append(fields[0])
                mag.append(fields[1])
                location.append(fields[2])
                county.append(fields[3])
                state.append(fields[4])
                lat.append(fields[5])
                lon.append(fields[6])
                comment.append(fields[7])

            storms = DataFrame({'Time': time, mag_string: mag, 'Location': location,
                                'County': county, 'State': state, 'Lat': lat,
                                'Lon': lon, 'Comment': comment})
            return(storms)
