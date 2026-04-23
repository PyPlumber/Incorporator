from pokemon import POKEMON_API

langList = POKEMON_API.Language.refresh()
print(langList[1])

## pokeList = POKEMON_API.PokeSpecies.refreshDataJSON(POKEMON_API.PokeSpecies.endpointAPI, rPath='results', nextUrlPath=['next'])
pokeList = POKEMON_API.PokeSpecies.refresh()

print(len(pokeList.values()))
print(pokeList[3].name)
print(pokeList[121].name)

pokeList[125].displayInfo()

pokeList[900].displayInfo()

pokeList[2].displayInfo(True)