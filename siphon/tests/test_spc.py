"""Test Storm Prediction Center access."""

import pytest

from numpy.testing import assert_almost_equal


from siphon.simplewebservice.spc import SpcData
from siphon.testing import get_recorder



recorder = get_recorder(__file__)


@recorder.use_cassette('spc_wind_archive')
def test_wind_archive():
    """Test that we are properly parsing wind data from the SPC archive."""
    # Testing wind events for random day in may 20th, 2013
    spc_wind_before_2018 = SpcData('wind', '20130520')

    assert(spc_wind_before_2018.storm_type == 'wind')
    assert(spc_wind_before_2018.date_time == '20130520')
    assert(spc_wind_before_2018.year_string == '2013')
    assert(spc_wind_before_2018.month_string == '05')
    assert(spc_wind_before_2018.day_string == '20')

    assert(spc_wind_before_2018.day_table['Num'].iloc[0] == 441595)
    assert(spc_wind_before_2018.day_table['Year'].iloc[0] == 2013)
    assert(spc_wind_before_2018.day_table['Month'].iloc[0] == 5)
    assert(spc_wind_before_2018.day_table['Day'].iloc[0] == 20)
    assert(spc_wind_before_2018.day_table['Time'].iloc[0] == '00:05:00')
    assert(spc_wind_before_2018.day_table['Time Zone'].iloc[0] == 3)
    assert(spc_wind_before_2018.day_table['State'].iloc[0] == 'MO')
    assert(spc_wind_before_2018.day_table['Speed (kt)'].iloc[0] == 56)
    assert(spc_wind_before_2018.day_table['Injuries'].iloc[0] == 0)
    assert(spc_wind_before_2018.day_table['Fatalities'].iloc[0] == 0)
    assert(spc_wind_before_2018.day_table['Property Loss'].iloc[0] == 0.005)
    assert(spc_wind_before_2018.day_table['Crop Loss'].iloc[0] == 0)
    assert(spc_wind_before_2018.day_table['Length (mi)'].iloc[0] == 0)
    assert(spc_wind_before_2018.day_table['Width (yd)'].iloc[0] == 0)
    assert(spc_wind_before_2018.day_table['Ns'].iloc[0] == 0)
    assert(spc_wind_before_2018.day_table['SN'].iloc[0] == 0)
    assert(spc_wind_before_2018.day_table['SG'].iloc[0] == 0)
    assert(spc_wind_before_2018.day_table['County Code 1'].iloc[0] == 167)
    assert(spc_wind_before_2018.day_table['County Code 2'].iloc[0] == 0)
    assert(spc_wind_before_2018.day_table['County Code 3'].iloc[0] == 0)
    assert(spc_wind_before_2018.day_table['County Code 4'].iloc[0] == 0)
    assert_almost_equal(spc_wind_before_2018.day_table['Start Lat'].iloc[0], 37.63, 3)
    assert_almost_equal(spc_wind_before_2018.day_table['Start Lon'].iloc[0], -93.42, 3)
    assert_almost_equal(spc_wind_before_2018.day_table['End Lat'].iloc[0], 37.63,3)
    assert_almost_equal(spc_wind_before_2018.day_table['End Lon'].iloc[0], -93.42, 3)


@recorder.use_cassette('spc_torn_archive')
def test_torn_archive():
    """Test that we are properly parsing tornado data from the SPC archive."""
    spc_torn_before_2018 = SpcData('tornado', '20130520')

    assert(spc_torn_before_2018.storm_type == 'tornado')
    assert(spc_torn_before_2018.date_time == '20130520')
    assert(spc_torn_before_2018.year_string == '2013')
    assert(spc_torn_before_2018.month_string == '05')
    assert(spc_torn_before_2018.day_string == '20')

    assert(spc_torn_before_2018.day_table['Num'].iloc[0] == 451537)
    assert(spc_torn_before_2018.day_table['Year'].iloc[0] == 2013)
    assert(spc_torn_before_2018.day_table['Month'].iloc[0] == 5)
    assert(spc_torn_before_2018.day_table['Day'].iloc[0] == 20)
    assert(spc_torn_before_2018.day_table['Time'].iloc[0] == '13:56:00')
    assert(spc_torn_before_2018.day_table['Time Zone'].iloc[0] == 3)
    assert(spc_torn_before_2018.day_table['State'].iloc[0] == 'OK')
    assert(spc_torn_before_2018.day_table['F-Scale'].iloc[0] == 5)
    assert(spc_torn_before_2018.day_table['Injuries'].iloc[0] == 212)
    assert(spc_torn_before_2018.day_table['Fatalities'].iloc[0] == 24)
    assert(spc_torn_before_2018.day_table['Property Loss'].iloc[0] == 2000)
    assert(spc_torn_before_2018.day_table['Crop Loss'].iloc[0] == 0)
    assert(spc_torn_before_2018.day_table['Length (mi)'].iloc[0] == 13.85)
    assert(spc_torn_before_2018.day_table['Width (yd)'].iloc[0] == 1900)
    assert(spc_torn_before_2018.day_table['Ns'].iloc[0] == 1)
    assert(spc_torn_before_2018.day_table['SN'].iloc[0] == 1)
    assert(spc_torn_before_2018.day_table['SG'].iloc[0] == 1)
    assert(spc_torn_before_2018.day_table['County Code 1'].iloc[0] == 87)
    assert(spc_torn_before_2018.day_table['County Code 2'].iloc[0] == 27)
    assert(spc_torn_before_2018.day_table['County Code 3'].iloc[0] == 0)
    assert(spc_torn_before_2018.day_table['County Code 4'].iloc[0] == 0)
    assert_almost_equal(spc_torn_before_2018.day_table['Start Lat'].iloc[0], 35.284, 3)
    assert_almost_equal(spc_torn_before_2018.day_table['Start Lon'].iloc[0], -97.628, 3)
    assert_almost_equal(spc_torn_before_2018.day_table['End Lat'].iloc[0], 35.341,3)
    assert_almost_equal(spc_torn_before_2018.day_table['End Lon'].iloc[0], -97.3999, 3)


@recorder.use_cassette('spc_hail_archive')
def test_hail_archive():
    """Test that we are properly parsing hail data from the SPC archive."""
    spc_hail_before_2018 = SpcData('hail', '20130520')

    assert(spc_hail_before_2018.storm_type == 'hail')
    assert(spc_hail_before_2018.date_time == '20130520')
    assert(spc_hail_before_2018.year_string == '2013')
    assert(spc_hail_before_2018.month_string == '05')
    assert(spc_hail_before_2018.day_string == '20')

    assert(spc_hail_before_2018.day_table['Num'].iloc[0] == 442000)
    assert(spc_hail_before_2018.day_table['Year'].iloc[0] == 2013)
    assert(spc_hail_before_2018.day_table['Month'].iloc[0] == 5)
    assert(spc_hail_before_2018.day_table['Day'].iloc[0] == 20)
    assert(spc_hail_before_2018.day_table['Time'].iloc[0] == '01:10:00')
    assert(spc_hail_before_2018.day_table['Time Zone'].iloc[0] == 3)
    assert(spc_hail_before_2018.day_table['State'].iloc[0] == 'KS')
    assert(spc_hail_before_2018.day_table['Size (hundredth in)'].iloc[0] == 0.88)
    assert(spc_hail_before_2018.day_table['Injuries'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['Fatalities'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['Property Loss'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['Crop Loss'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['Length (mi)'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['Width (yd)'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['Ns'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['SN'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['SG'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['County Code 1'].iloc[0] == 19)
    assert(spc_hail_before_2018.day_table['County Code 2'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['County Code 3'].iloc[0] == 0)
    assert(spc_hail_before_2018.day_table['County Code 4'].iloc[0] == 0)
    assert_almost_equal(spc_hail_before_2018.day_table['Start Lat'].iloc[0], 37.13, 3)
    assert_almost_equal(spc_hail_before_2018.day_table['Start Lon'].iloc[0], -96.18, 3)
    assert_almost_equal(spc_hail_before_2018.day_table['End Lat'].iloc[0], 37.13,3)
    assert_almost_equal(spc_hail_before_2018.day_table['End Lon'].iloc[0], -96.18, 3)


@recorder.use_cassette('spc_wind_after_2018_archive')
def test_wind_after_2018_archive():
    """Test of method of SPC wind data parscing to be used for recent years."""
    # Second set of assertions for parscing done for storm events after 12/31/2017
    spc_wind_after_2018 = SpcData('wind', '20180615')

    assert(spc_wind_after_2018.day_table['Time'].iloc[0] == 1215)
    assert(spc_wind_after_2018.day_table['Speed (kt)'].iloc[0] == '78')
    assert(spc_wind_after_2018.day_table['Location'].iloc[0] == '13 SSE LITTLE MARAIS')
    assert(spc_wind_after_2018.day_table['County'].iloc[0] == 'LSZ162')
    assert(spc_wind_after_2018.day_table['State'].iloc[0] == 'MN')
    assert_almost_equal(spc_wind_after_2018.day_table['Lat'].iloc[0], 47.25, 3)
    assert_almost_equal(spc_wind_after_2018.day_table['Lon'].iloc[0], -90.96, 3)
    assert(spc_wind_after_2018.day_table['Comment'].iloc[0] == 'REPORTED BY THE JAMES R. BARKER. (DLH)')

@recorder.use_cassette('spc_torn_after_2018_archive')
def test_torn_after_2018_archive():
    """Test of method of SPC tornado data parscing to be used for recent years."""
    spc_torn_after_2018 = SpcData('tornado', '20180615')

    assert(spc_torn_after_2018.day_table['Time'].iloc[0] == 2225)
    assert(spc_torn_after_2018.day_table['F-Scale'].iloc[0] == 'UNK')
    assert(spc_torn_after_2018.day_table['Location'].iloc[0] == '5 W WYNNE')
    assert(spc_torn_after_2018.day_table['County'].iloc[0] == 'CROSS')
    assert(spc_torn_after_2018.day_table['State'].iloc[0] == 'AR')
    assert_almost_equal(spc_torn_after_2018.day_table['Lat'].iloc[0], 35.23, 3)
    assert_almost_equal(spc_torn_after_2018.day_table['Lon'].iloc[0], -90.88, 3)
    assert(spc_torn_after_2018.day_table['Comment'].iloc[0][0] == 'A')


@recorder.use_cassette('spc_hail_after_2018_archive')
def test_hail_after_2018_archive():
    """Test of method of SPC hail data parscing to be used for recent years."""
    spc_hail_after_2018 = SpcData('hail', '20180615')
    
    assert(spc_hail_after_2018.day_table['Time'].iloc[0] == 1335)
    assert(spc_hail_after_2018.day_table['Location'].iloc[0] == 'MOSINEE')
    assert(spc_hail_after_2018.day_table['County'].iloc[0] == 'MARATHON')
    assert(spc_hail_after_2018.day_table['State'].iloc[0] == 'WI')
    assert_almost_equal(spc_hail_after_2018.day_table['Lat'].iloc[0], 44.78, 3)
    assert_almost_equal(spc_hail_after_2018.day_table['Lon'].iloc[0], -89.69, 3)
    assert(spc_hail_after_2018.day_table['Comment'].iloc[0] == '(GRB)')


@recorder.use_cassette('spc_no_data')
def test_no_data_spc():
    """Test spc data when passed an invalid storm type ."""
    with pytest.raises(ValueError):
        SpcData('hotdog', '19650403')
