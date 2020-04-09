from lambda_function import (
    state_stream,
    county_stream,
    STATE,
    COUNTY,
    DEATHS,
    POPULATION,
    CASES,
    COLORADO,
    GARFIELD,
    GARFIELD_POPULATION,
)


def test_state_stream_filters_out_non_colorado_states():
    stream = [
        {STATE: COLORADO, DEATHS: 1, CASES: 1},
        {STATE: 'not-colorado', DEATHS: 1, CASES: 1},
    ]
    assert len([state_stream(stream)]) == 1


def test_state_stream_adds_population_field():
    stream = [
        {STATE: COLORADO, DEATHS: 1, CASES: 1},
    ]
    assert next(state_stream(stream))[POPULATION] != 0


def test_county_stream_filters_out_non_county_entries():
    stream = [
        {COUNTY: GARFIELD, STATE: COLORADO, DEATHS: 1, CASES: 1},
        {COUNTY: GARFIELD, STATE: 'not-colorado', DEATHS: 1, CASES: 1},
        {COUNTY: 'not-garfield', STATE: COLORADO, DEATHS: 1, CASES: 1},
        {COUNTY: 'not-garfield', STATE: 'not-colorado', DEATHS: 1, CASES: 1},
    ]
    assert len([county_stream(GARFIELD, GARFIELD_POPULATION)(stream)]) == 1


def test_county_stream_adds_population_field():
    stream = [
        {COUNTY: GARFIELD, STATE: COLORADO, DEATHS: 1, CASES: 1},
    ]
    assert next(county_stream(GARFIELD, GARFIELD_POPULATION)(stream)) != 0
