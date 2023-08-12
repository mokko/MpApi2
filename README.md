# MpApi - Unofficial asynchronous API client for Zetcom's MuseumPlus

API Specification: http://docs.zetcom.com/ws

## Utility / Modules
* Provides a cli utility 'monk', plus the following modules
* MpApi.aio.chunky
* MpApi.aio.client (only a few endpoints implemented at the moment)
* MpApi.aio.session

## Usage
> monk -j jobname

## DSL Format
```
conf:
	chunkSize 800 # comment
	exclude_modules ObjectGroup
	chunks 2 # parallel chunks
	semaphore 10 
ajob:
	apack group 1234 # possible query types: approval, exhibit, group, loc, query
	query 429068 Object # run a saved query with the given id that gets back Object
```

* 'ajob' is a job label; 
* 'apack' and 'query' are commands, 
* the rest are parameters and comments. 
* 'apack' downloads a set of objects, plus related data (except for specifically excluded
  modules): apack {qtype} {ID} where qtype stands for query type and ID is an int. These
  Possible query types are: approval [group], exhib[ition], group, loc[ation]. 
* 'query' executes a saved query: query {ID} {target} where the int ID describes the 
  saved query and names the module type (mtype) of the items to get.

## Requirements
* Python (probably Python 3.9 for asyncio) 
* lxml
* aiohttp
* MpApi (https://github.com/mokko/MpApi)

For Testing
* pytest 
* pytest-asyncio

## Version History
* 20221228 - created
* 20230811 - version 0.0.2 minimal working version with parallel chunks,
			 command 'query' added
