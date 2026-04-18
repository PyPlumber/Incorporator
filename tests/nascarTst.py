import NASCAR_API

drivers = NASCAR_API.DriverBase.refreshDataREST(rPath='response')
# trackList  = NASCAR_API.TrackBase.refreshDataREST(NASCAR_API.TrackBase.endpointAPI, rPath='items')
# nascrRaces = NASCAR_API.NascarRaceBase.refreshDataREST(NASCAR_API.NascarRaceBase.endpointAPI, rPath='series_1')
# buschRaces = NASCAR_API.BuschRaceBase.refreshDataREST(NASCAR_API.BuschRaceBase.endpointAPI, rPath='series_2')
# truckRaces = NASCAR_API.TruckRaceBase.refreshDataREST(NASCAR_API.TruckRaceBase.endpointAPI, rPath='series_3')
nascarStandings = NASCAR_API.NascarStandings.refreshDataREST()
buschStandings = NASCAR_API.BuschStandings.refreshDataREST()
truckStandings = NASCAR_API.TruckStandings.refreshDataREST()

league_teams_raw = {
    "King": [(3.0, 4235), (2.0, 4441), (1.0, 3989), (1.0, 4062), (1.0, 4123), (1.0, 4272), (1.0, 3859), (1.0, 4481)],
    "Intim'tor": [(3.0, 4312), (2.0, 34), (1.0, 4030), (1.0, 4023), (1.0, 3989), (1.0, 4153), (1.0, 4065), (1.0, 4481)],
    "WonderBoy": [(3.0, 4235), (2.0, 4133), (1.0, 4153), (1.0, 4030), (1.0, 1816), (1.0, 4065), (1.0, 3859), (1.0, 4481)],
    "AlabamaG": [(3.0, 4446), (2.0, 34), (1.0, 4030), (1.0, 454), (1.0, 4023), (1.0, 4153), (1.0, 4065), (1.0, 4481)],
    "Jaws": [(3.0, 4446), (2.0, 34), (1.0, 4065), (1.0, 4030), (1.0, 4153), (1.0, 3859), (1.0, 4001), (1.0, 4481)],
    "Seven": [(3.0, 4235), (2.0, 4133), (1.0, 1816), (1.0, 454), (1.0, 4062), (1.0, 1361), (1.0, 3859), (1.0, 4481)],
    "Cale": [(3.0, 4427), (2.0, 4133), (1.0, 4023), (1.0, 4001), (1.0, 4153), (1.0, 4030), (1.0, 4065), (1.0, 4481)],
    "SilverFox": [(3.0, 4235), (2.0, 34), (1.0, 4023), (1.0, 3989), (1.0, 4062), (1.0, 4153), (1.0, 4469), (1.0, 4481)]
    }

points_standings = {1: nascarStandings, 2: buschStandings, 3: truckStandings}
league_roster = {player for roster in  league_teams_raw.values() for player in roster}

league_scores = {}
for driver in league_roster:
    league_scores[driver] = 0
    league_scores[driver] += points_standings[int(driver[0])][driver[1]].points

league_teams = {}
for teamCd, roster in league_teams_raw.items():
    league_teams[teamCd] = {}
    for series_id, driverId in roster:
        league_teams[teamCd].setdefault(series_id,[]).append(drivers[driverId])
    for series_id in range(1,4):
         league_teams[teamCd][series_id].sort(key=lambda x:int(x.Badge))

team_score = {}
for team, roster in  league_teams.items():
    print(f"Team {team}!")
    print("-------------")
    series_list = ('Cup', 'Busch', 'Truck')
    for series_id, series_name in enumerate(series_list,start=1):
        team_score[series_id] = {}
        team_score[series_id][team] = 0
        for idx, driver in enumerate(roster[series_id], start=1):
            team_score[series_id][team] += league_scores[(series_id,driver.code)]
            print(f'{{"Series": "{series_name:<7}", "Driver": "{idx:<2}", "name": "{driver.Full_Name:<20}", "car": "{driver.Badge:<4}", "team": "{driver.Team:<25}", "wins": {points_standings[series_id][int(driver.code)].wins:<2}, "t10": {points_standings[series_id][int(driver.code)].top_10:<2}, "points": {league_scores[(series_id,driver.code)]:<3}}},')

        # team_score[4] = {}
        # team_score[4][team] += team_score[series_id][team]
        print('\n')

    print(f"\nYou're team's total score is: {team_score[1][team]+team_score[2][team]+team_score[3][team]}","\n")