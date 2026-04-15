trackList  = TrackBase.refreshDataREST(TrackBase.endpointAPI, rPath='items', nextUrlPath=['next'])
nascrRaces = NascarRaceBase.refreshDataREST(NascarRaceBase.endpointAPI, rPath='series_1', nextUrlPath=['next'])
buschRaces = BuschRaceBase.refreshDataREST(BuschRaceBase.endpointAPI, rPath='series_2', nextUrlPath=['next'])
truckRaces = TruckRaceBase.refreshDataREST(TruckRaceBase.endpointAPI, rPath='series_3', nextUrlPath=['next'])


print(len(nascrRaces))