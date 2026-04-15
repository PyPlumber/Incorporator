from dateutil import parser
from datetime import datetime
import Incorporator

NascarBase = Incorporator.Incorporator.incSubCls('NascarBase', 'id', 'name',
    'https://cf.nascar.com/cacher/',
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

TrackBase = NascarBase.incSubCls('TrackBase','track_id', 'track_name',
    NascarBase.endpointAPI+'tracks.json',
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

RaceBase = NascarBase.incSubCls('RaceBase','race_id', 'race_name',
    NascarBase.endpointAPI+str(datetime.now().year)+'/race_list_basic.json',
    codeAdds= {},
    exclAdds=['schedule','track_name'],
    convAdds=dict({
        'date_scheduled': lambda x: parser.parse(x) if x else "",
        'race_date': lambda x: parser.parse(x) if x else "",
        'qualifying_date': lambda x: parser.parse(x) if x else "",
        'tunein_date': lambda x: parser.parse(x) if x else "",
        'schedule.start_time_utc': lambda x: parser.parse(x) if x else ""
        }),
    nameAdds={})

NascarRaceBase = NascarBase.incSubCls('RaceBase','race_id', 'race_name',
    RaceBase.endpointAPI,
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

BuschRaceBase = NascarBase.incSubCls('RaceBase','race_id', 'race_name',
    RaceBase.endpointAPI,
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

TruckRaceBase = NascarBase.incSubCls('RaceBase','race_id', 'race_name',
    RaceBase.endpointAPI,
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

DriverBase = NascarBase.incSubCls('DriverBase','Nascar_Driver_ID', 'Full_Name',
    NascarBase.endpointAPI+'drivers.json',
    codeAdds= {},
    exclAdds=[],
    convAdds=dict({
        'DOB': lambda x: parser.parse(x) if x else "",
        'DOD': lambda x: parser.parse(x) if x else ""
    }),
    nameAdds={})

NascarStandings = NascarBase.incSubCls('NascarStandings','position', 'driver_name',
    'https://cf.nascar.com/data/cacher/production/'+str(datetime.now().year)+'/1/racinginsights-points-feed.json',
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

BuschStandings = NascarBase.incSubCls('BuschStandings','position', 'driver_name',
    'https://cf.nascar.com/data/cacher/production/'+str(datetime.now().year)+'/2/racinginsights-points-feed.json',
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

TruckStandings = NascarBase.incSubCls('TruckStandings','position', 'driver_name',
    'https://cf.nascar.com/data/cacher/production/'+str(datetime.now().year)+'/3/racinginsights-points-feed.json',
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})




