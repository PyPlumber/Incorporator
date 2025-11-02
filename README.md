# Incorporator

The Incorporator is a Python "Super" Class desgined to quickly automate retrieval and conversion of data records from an external source.  Create a subclass of the Incorporator and populate it with instances of the external data records.  The subclass instances contain the converted data attributes as properties.  Subclass properties can store hashmap keys to related data records stored in a different Incorporator subclass.

## Description

The algorithm will:
  Use PANDAS to parse JSON into DataFrames for manipulation.

  Create a Class for the data records to reference attributes
  
  Create a Class Dictionary for a given unique data record index/key/field
     
  Dynamically name Class properties for the incoming field or attributes names
  
  Convert values to primitive Python Classes or Dictionary pointers
  
  Change or override incoming field/attribute field names

## Getting Started

The current implementation for REST API requires only these inputs: url, instance key , instance name.

### Dependencies

PANDAS for JSON Normalize, requests for JSON API call, copy for Dictionaries, dateutil for parser

### Installing

### Executing program

* Create New Incorporator SubClass
* Run <New Incorporator SubClass>.refreshDataREST(nextURL, rPath)
```
Review Constants_API.py for SubClass creation
Then use dictionary reference and attribute name for direct references, example: charList[4].origin
```

## Help

Rick and Mory API shows relational capabilities of loading with Incorporator
Pokemon API test shows flexibility working with larger datasets quickly.
```

```

## Authors


## Version History

* 0.1
    * Initial Release

## License


## Acknowledgments

Inspiration, code snippets, etc.
* [Rick And Morty API Docs](https://rickandmortyapi.com/documentation)
