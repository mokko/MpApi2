"""
Let's investigate which related modules I can find in our data.
"""
from lxml import etree
from mpapi.module import Module

source = "C:/m3/MpApi-Replacer/sdata/temp485397.zml.xml"
m = Module(file=source)
resultL = m.xpath(
    "/m:application/m:modules/m:module/m:moduleItem/m:moduleReference[@targetModule]"
)
targets = set()
for result in resultL:
    targets.add(result.xpath("@targetModule")[0])
    # print(f"{result.xpath('@targetModule')[0]}") # {result.xpath('@name')[0]}
    # xml = etree.tostring(
    # result, pretty_print=True, encoding="unicode"
    # )
    # print (xml)
print(targets)
