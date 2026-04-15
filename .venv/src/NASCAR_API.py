from dateutil import parser
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
    NascarBase.endpointAPI+'2026/'+'race_list_basic.json',
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

# trackList  = TrackBase.refreshDataREST(TrackBase.endpointAPI, rPath='items', nextUrlPath=['next'])
# nascrRaces = NascarRaceBase.refreshDataREST(NascarRaceBase.endpointAPI, rPath='series_1', nextUrlPath=['next'])
# buschRaces = BuschRaceBase.refreshDataREST(BuschRaceBase.endpointAPI, rPath='series_2', nextUrlPath=['next'])
# truckRaces = TruckRaceBase.refreshDataREST(TruckRaceBase.endpointAPI, rPath='series_3', nextUrlPath=['next'])

# # NASCAR API Initialization
# API_URL = "https://cf.nascar.com/live/feeds/live-feed.json"
# nascar_feed = NASCARFeed(API_URL)
#
# # Other URLS
# # https://cf.nascar.com/cacher/2024/2/5451/weekend-feed.json
# # https://cf.nascar.com/cacher/2024/2/5451/live-stage-points.json
# # https://cf.nascar.com/cacher/2024/2/5451/lap-averages.json
# # https://cf.nascar.com/cacher/tracks.json
# # https://cf.nascar.com/cacher/2024/race_list_basic.json
#
# MANU_POINTS_URL = "https://cf.nascar.com/cacher/2022/1/final/1-manufacturer-points.json" # url to pull manufacturer points
# OWNERS_POINTS_URL = "https://cf.nascar.com/cacher/2022/1/final/1-owners-points.json" # url to pull owners points
# DRIVERS_POINTS_URL = "https://cf.nascar.com/cacher/2022/1/final/1-drivers-points.json" # url to pull drivers and driver points
# RACE_RESULTS_URL = f"https://cf.nascar.com/cacher/2022/1/{curr_race_id}/weekend-feed.json" # url to pull race results
# ADVANCED_DRIVER_STATS_URL = f"https://cf.nascar.com/cacher/2022/1/deep-driver-stats.json" # url to pull advanced driver stats
# LIVE_FEED_URL = "https://cf.nascar.com/cacher/live/live-feed.json" # url to pull live feed data

# https://cf.nascar.com/cacher/2026/race_list_basic.json

# https://cf.nascar.com/live-ops/live-ops.json
# https://cf.nascar.com/cacher/live/live-feed.json

# get_season_schedule
# https://cf.nascar.com/cacher/{year}/race_list_basic.json

# get next, finished races
# https://cf.nascar.com/cacher/{datetime.now().year}/race_list_basic.json

# get_race_results
# https://cf.nascar.com/data/cacher/production/{year}/{series}/{race_id}/raceResults.json

# get_points_standings
# https://cf.nascar.com/data/cacher/production/{year}/{series}/racinginsights-points-feed.json

# all_drivers_info
# https://cf.nascar.com/cacher/drivers.json

# get_owners_points
# https://cf.nascar.com/cacher/{year}/{series}/final/{series}-owners-points.json

# get_manufacturer_points
# https://cf.nascar.com/cacher/{year}/{series}/final/{series}-manufacturer-points.json

# get_pit_data
# https://cf.nascar.com/cacher/live/series_{series}/{race_id}/live-pit-data.json

