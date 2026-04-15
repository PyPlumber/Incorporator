import NASCAR_API

trackList  = NASCAR_API.TrackBase.refreshDataREST(NASCAR_API.TrackBase.endpointAPI, rPath='items', nextUrlPath=['next'])
nascrRaces = NASCAR_API.NascarRaceBase.refreshDataREST(NASCAR_API.NascarRaceBase.endpointAPI, rPath='series_1', nextUrlPath=['next'])
buschRaces = NASCAR_API.BuschRaceBase.refreshDataREST(NASCAR_API.BuschRaceBase.endpointAPI, rPath='series_2', nextUrlPath=['next'])
truckRaces = NASCAR_API.TruckRaceBase.refreshDataREST(NASCAR_API.TruckRaceBase.endpointAPI, rPath='series_3', nextUrlPath=['next'])

print(len(buschRaces))

drivers = NASCAR_API.DriverBase.refreshDataREST(NASCAR_API.DriverBase.endpointAPI, rPath='response', nextUrlPath=['next'])

print(len(drivers))

print(drivers[4065].DOB.month)
print(drivers[4065].Team)

nascarStandings = NASCAR_API.NascarStandings.refreshDataREST(NASCAR_API.NascarStandings.endpointAPI)

print(len(nascarStandings))

print(nascarStandings[4].name)

for pos, driver in nascarStandings.items():
    if pos is None:
        continue
    print(f"{driver.name} ({drivers[driver.driver_id].Twitter_Handle}) is in {pos} position with {driver.points}.")
