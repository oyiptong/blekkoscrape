#!/usr/bin/env python

from lxml.html.soupparser import fromstring
import pycurl
import json
import cStringIO as StringIO
import urllib
import re
import os
import logging

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

PARSED = set()

urls = [
        "https://blekko.com/ws/+/view+/blekko/comedy",
        "https://blekko.com/ws/+/view+/blekko/objective-c",
        "https://blekko.com/ws/+/view+/blekko/psych",
        "https://blekko.com/ws/+/view+/blekko/news",
        "https://blekko.com/ws/+/view+/blekko/hotels+",
        "https://blekko.com/ws/+/view+/blekko/kiteboarding",
        "https://blekko.com/ws/+/view+/blekko/national-parks",
        "https://blekko.com/ws/+/view+/blekko/made-in-america",
        "https://blekko.com/ws/+/view+/blekko/epl",
]

INTERNAL_LINK = re.compile(r'^/blekko')

def parse(url):
    logger.info("parsing {0}".format(url))

    label = url.split('/')[-1]
    output = {
            "name" : label,
            "urls" : []
    }

    if label in PARSED:
        return output

    PARSED.add(label)

    fp = StringIO.StringIO()
    curl = pycurl.Curl()
    curl.setopt(pycurl.URL, url)
    curl.setopt(pycurl.FOLLOWLOCATION, 1)
    curl.setopt(pycurl.MAXREDIRS, 5)
    curl.setopt(pycurl.CONNECTTIMEOUT, 30)
    curl.setopt(pycurl.TIMEOUT, 300)
    curl.setopt(pycurl.NOSIGNAL, 1)
    curl.setopt(pycurl.WRITEFUNCTION, fp.write)
    curl.perform()
    curl.close()

    page_str = fp.getvalue()
    fp.close()
    try:
        root = fromstring(page_str)
    except ValueError, e:
        logger.error("Error parsing url: {0} error: {1}".format(url, e.message))
        return output

    textarea_elems = root.xpath("//textarea[@id='urls-text']")

    if textarea_elems:
        links = textarea_elems[0].text_content().split()
        for link in links:
            if INTERNAL_LINK.match(link):

                # make n-level subtopics first class
                subcat_url = "https://blekko.com/ws/+/view+{0}".format(link)
                global urls
                if link not in PARSED and subcat_url not in urls:
                    urls.append(subcat_url)
            else:
                output["urls"].append(link)

    return output

def main():
    output = [
    ]

    while urls:
        data = parse(urls.pop())
        if data["urls"]:
            output.append(data)

    names = [d["name"] for d in output]
    logging.info("updating list with {0} new categories: {1}".format(len(output), ", ".join(names)))

    dataset = {}

    with open('./out.json', 'r') as f:
        dataset = json.load(f)
        dataset["d"].extend(output)

    with open('./blekko.json', 'w') as f:
        json.dump(dataset, f, indent=4, sort_keys=True)


if __name__ == '__main__':
    main()
