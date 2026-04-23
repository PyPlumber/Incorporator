import incorporator as Incorp
from datetime import datetime

def convDriverSeries(series_text):
    match series_text:
        case 'nascar-craftsman-truck-series':
            return 3
        case 'nascar-oreilly-auto-parts-series':
            return 2
        case 'nascar-cup-series':
            return 1

    return None

NascarBase = Incorp.Incorporator.incorp('NascarBase', 'id', 'name',
    url='https://cf.nascar.com/cacher/',
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

TrackBase = NascarBase.incorp('TrackBase', 'track_id', 'track_name',
    url=NascarBase.url +'tracks.json',
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

DriverBase = NascarBase.incorp('DriverBase', 'Nascar_Driver_ID', 'Full_Name',
    url=NascarBase.url +'drivers.json',rPath='response',
    codeAdds= {},
    exclAdds=[
        'Series_Logo','Short_Name','Description','Hobbies','Children','Residing_City',
        'Residing_State','Residing_Country','Image_Transparent','SecondaryImage','Career_Stats',
        'Age','Rank','Points','Points_Behind','No_Wins','Poles','Top5','Top10','Laps_Led',
        'Stage_Wins','Playoff_Points','Playoff_Rank','Integrated_Sponsor_Name','Integrated_Sponsor',
        'Integrated_Sponsor_URL','Silly_Season_Change','Silly_Season_Change_Description',
        'Driver_Post_Status','Driver_Part_Time'
        ],
    convAdds=dict({
        'DOB': lambda x: NascarBase.parseDateTime(x),
        'DOD': lambda x: NascarBase.parseDateTime(x),
        'Driver_Series': lambda x: convDriverSeries(x) if x else None
        }),
    nameAdds={})

RaceBase = NascarBase.incorp('RaceBase', 'race_id', 'race_name',
    url=NascarBase.url + str(datetime.now().year) +'/race_list_basic.json',
    codeAdds= {},
    exclAdds=['schedule','track_name'],
    convAdds=dict({
        'track_id': lambda x: TrackBase.codeDict.get(x, None),
        'date_scheduled': lambda x: NascarBase.parseDateTime(x),
        'race_date': lambda x: NascarBase.parseDateTime(x),
        'qualifying_date': lambda x: NascarBase.parseDateTime(x),
        'tunein_date': lambda x: NascarBase.parseDateTime(x),
        'pole_winner_driver_id': lambda x: DriverBase.codeDict.get(x, None)
        }),
    nameAdds={
        'track_id': 'track'
        })

NascarRaceBase = RaceBase.incorp('NascarRaceBase', 'race_id', 'race_name',
    url=RaceBase.url,
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

BuschRaceBase = RaceBase.incorp('BuschRaceBase', 'race_id', 'race_name',
    url=RaceBase.url,
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

TruckRaceBase = RaceBase.incorp('TruckRaceBase', 'race_id', 'race_name',
    url=RaceBase.url,
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

StandingsBase = NascarBase.incorp('NascarStandings', 'driver_id', 'driver_name',
    url=f"https://cf.nascar.com/data/cacher/production/{datetime.now().year}/",
    codeAdds={},
    exclAdds=[
        'delta_playoff','is_clinch','starts','poles',
        'driver_first_name', 'driver_last_name', 'driver_suffix'
        ],
    convAdds={},
    nameAdds={})


NascarStandings = StandingsBase.incorp('NascarStandings', 'driver_id', 'driver_name',
    url=StandingsBase.url +'1/racinginsights-points-feed.json',
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})


TruckStandings = StandingsBase.incorp('NascarStandings', 'driver_id', 'driver_name',
    url=StandingsBase.url +'3/racinginsights-points-feed.json',
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

BuschStandings = StandingsBase.incorp('NascarStandings', 'driver_id', 'driver_name',
    url=StandingsBase.url +'2/racinginsights-points-feed.json',
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})