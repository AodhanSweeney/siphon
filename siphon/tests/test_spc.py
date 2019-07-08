"""Test Storm Prediction Center access."""

from datetime import datetime
from numpy.testing import assert_almost_equal
import pytest

from siphon.simplewebservice.spc import SpcData
from siphon.testing import get_recorder


recorder = get_recorder(__file__)


@recorder.use_cassette('spc_archive')
def test_spc():
    """Test that we are properly parsing data from the Storm Prediction Center."""
    # Testing wind events for random day in may 20th, 2013
    spc_before_2018 = SpcData('wind', '20130520')
    assert(spc_before_2018.day_table['Num'].iloc[0] == 441595)
    assert(spc_before_2018.day_table['Year'].iloc[0] == 2013)
    assert(spc_before_2018.day_table['Month'].iloc[0] == 5)
    assert(spc_before_2018.day_table['Day'].iloc[0] == 20)
    assert(spc_before_2018.day_table['Time'].iloc[0] == '00:05:00')
    assert(spc_before_2018.day_table['Time Zone'].iloc[0] == 3)
    assert(spc_before_2018.day_table['State'].iloc[0] == 'MO')
    assert(spc_before_2018.day_table['Injuries'].iloc[0] == 0)
    assert(spc_before_2018.day_table['Fatalities'].iloc[0] == 0)
    assert(spc_before_2018.day_table['Property Loss'].iloc[0] == 0.005)
    assert(spc_before_2018.day_table['Crop Loss'].iloc[0] == 0)
    assert(spc_before_2018.day_table['Start Lat'].iloc[0] == 37.63)
    assert(spc_before_2018.day_table['Start Lon'].iloc[0] == -93.42)
    assert(spc_before_2018.day_table['End Lat'].iloc[0] == 37.63)
    assert(spc_before_2018.day_table['End Lon'].iloc[0] == -93.42)
    assert(spc_before_2018.day_table['Length (mi)'].iloc[0] == 0)
    assert(spc_before_2018.day_table['Width (yd)'].iloc[0] == 0)
    assert(spc_before_2018.day_table['Ns'].iloc[0] == 0)
    assert(spc_before_2018.day_table['SN'].iloc[0] == 0)
    assert(spc_before_2018.day_table['SG'].iloc[0] == 0)
    assert(spc_before_2018.day_table['County Code 1'].iloc[0] == 167)
    assert(spc_before_2018.day_table['County Code 2'].iloc[0] == 0)
    assert(spc_before_2018.day_table['County Code 3'].iloc[0] == 0)
    assert(spc_before_2018.day_table['County Code 4'].iloc[0] == 0)

@recorder.use_cassette('spc_after_2018_archive')
def test_spc_new_parcing_method():
    """Test of method of SPC data parscing to be used for recent years."""
    # Second set of assertions for parscing done for storm events after 12/31/2017
    spc_after_2018 = SpcData('hail', '20180615')
    assert(spc_after_2018.day_table['Time'].iloc[0] == 1335)
    assert(spc_after_2018.day_table['Location'].iloc[0] == 'MOSINEE')
    assert(spc_after_2018.day_table['County'].iloc[0] == 'MARATHON')
    assert(spc_after_2018.day_table['State'].iloc[0] == 'WI')
    assert(spc_after_2018.day_table['Lat'].iloc[0] == 44.78)
    assert(spc_after_2018.day_table['Lon'].iloc[0] == -89.69)
    assert(spc_after_2018.day_table['Comment'].iloc[0] == '(GRB)')


test_spc()
test_spc_new_parcing_method()
