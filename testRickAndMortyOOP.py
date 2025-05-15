import pandas as pd
import Constants_API

# print(vars(Constants_API.RickAndMortyAPI))
# print(vars(Constants_API.Location))
# print(vars(Constants_API.Episode))
# print(vars(Constants_API.Character))

locList = Constants_API.Location.refreshDataREST(Constants_API.Location.endpointAPI, rPath='results', nextUrlPath=['info','next'])
print(locList[21])
print(locList[16].name)
print("\n")

charList = Constants_API.Character.refreshDataREST(Constants_API.Character.endpointAPI, rPath='results', nextUrlPath=['info','next'])
print(charList[4])
print(charList[4].gender)
print(charList[4].origin)
charList[4].origin.displayInfo()

print("\n")

epsList = Constants_API.Episode.refreshDataREST(Constants_API.Episode.endpointAPI, rPath='results', nextUrlPath=['info','next'])
print(epsList[4])

print("\n")

story = epsList[7]
print(f"Episode {story.code} was titled {story.name}.")
print(f"The episode aired on {story.air_date.strftime('%d, %b %Y')}.")
print(f"The API instance was created in {story.created.strftime('%Y')}.")

print("\n")

cast = story.characters
print(f"It had {len(cast)} characters:")
for character in cast:
    character.displayInfo()


print("\n")

origins = []
for character in cast:
    origins.append([character.name, character.origin.name])

originsDF = pd.DataFrame(origins, columns =['Character', 'Origin'])
print(f"Here is episode {story.code}'s character origin distribution:")
print(originsDF.value_counts('Origin'))
