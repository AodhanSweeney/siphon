"""Test National Hurricane Center database access."""

import pytest

from siphon.simplewebservice.nhc import NHCD
from siphon.testing import get_recorder


recorder = get_recorder(__file__)


@recorder.use_cassette('nhc_database')
def test_nhc():
    """Test that we are properly parsing data from the National Hurricane Center Database."""
    nhc = NHCD()
    assert(nhc.storm_table['Name'].loc[0] == '   UNNAMED')
    assert(nhc.storm_table['Basin'].loc[0] == ' AL')
    assert(nhc.storm_table['CycloneNum'].loc[0] == 1)
    assert(nhc.storm_table['Year'].loc[0] == 1851)
    assert(nhc.storm_table['StormType'].loc[0] == ' HU')
    assert(nhc.storm_table['Filename'].loc[0] == ' al011851')


@recorder.use_cassette('nhc_storm_archive')
def test_nhc_archives():
    """Test that storm dictionaries are being sifted and appended correctly."""
    # Perform test on Hurricane Anna from 1976
    nhc = NHCD()
    nhc.get_tracks(1976, ' al061976')

    # Asserting characteristics of first forecast for Anna
    assert(nhc.storm_dictionary['forecast']['Basin'].iloc[0] == 'AL')
    assert(nhc.storm_dictionary['forecast']['CycloneNum'].iloc[0] == 6)
    assert(nhc.storm_dictionary['forecast']['WarnDT'].iloc[0] == '1976072818')
    assert(nhc.storm_dictionary['forecast']['Model'].iloc[0] == ' BCD5')
    assert(nhc.storm_dictionary['forecast']['Forecast_hour'].iloc[0] == 0)
    assert(nhc.storm_dictionary['forecast']['Lat'].iloc[0] == 28)
    assert(nhc.storm_dictionary['forecast']['Lon'].iloc[0] == -52.3)

    # Asserting characteristics of best track for Anna
    assert(nhc.storm_dictionary['best_track']['Basin'].iloc[0] == 'AL')
    assert(nhc.storm_dictionary['best_track']['CycloneNum'].iloc[0] == 6)
    assert(nhc.storm_dictionary['best_track']['WarnDT'].iloc[0] == '1976072818')
    assert(nhc.storm_dictionary['best_track']['Model'].iloc[0] == ' BEST')
    assert(nhc.storm_dictionary['best_track']['Forecast_hour'].iloc[0] == 0)
    assert(nhc.storm_dictionary['best_track']['Lat'].iloc[0] == 28)
    assert(nhc.storm_dictionary['best_track']['Lon'].iloc[0] == -52.3)


@recorder.use_cassette('nhc_no_data')
def test_no_data_nhc():
    """Test nhc data when passed an invalid url."""
    with pytest.raises(ValueError):
        nhc.get_tracks(1965, 'ab123456')

test_nhc()
test_nhc_archives()
