# MpApi - Unofficial asynchronous API client for Zetcom's MuseumPlus

API Specification: http://docs.zetcom.com/ws

## Utility / Modules
* Provides a cli utility 'monk', plus the following modules
* MpApi.aio.client
* MpApi.aio.chunky

Usage
> monk -j jobname

## DSL Format
conf:
	chunkSize 1111 # comment
ajob:
	apack group 1234 

## Requirements
* Python 3.9
* lxml
* aiohttp
* mpapi.module
* mpapi.search

For Testing
* pytest 
* pytest-asyncio

# Version History
* 20221228 - created
* 20230804 - half of async working

# See Also
* https://github.com/mokko/MpApi