#!/usr/bin/env python
from publicsuffix import PublicSuffixList
import os
import json
from furl import furl
import re

WWW_PAT = re.compile(r'www\w*')

psl = PublicSuffixList()

def main():
    with open('./blekko.json', 'r') as f:
        in_data = json.load(f)

    out_data = {}

    for category in in_data["d"]:
        cat_name = category["name"]

        # cleanup cat name 
        if cat_name[-1] == "+":
            cat_name = cat_name[:-1]

        for url in category["urls"]:

            # make www.domain.tld collapse to domain.tld
            try:
                uri = furl(url)
            except ValueError:
                print "error with {0}".format(url)
                continue

            domain_name = psl.get_public_suffix(uri.host)
            out_hostname = uri.host

            if uri.host != domain_name:
                index = uri.host.index(domain_name)
                subdomains = uri.host[:index-1]
                if WWW_PAT.match(subdomains):
                    out_hostname = domain_name
            
            if not out_hostname:
                print "error with {0}".format(url)
                continue

            if str(uri.path) != "/":
                out_url = out_hostname + unicode(uri.path)
            else:
                out_url = out_hostname

            if out_data.has_key(out_url):
                if cat_name not in out_data[out_url]:
                    out_data[out_url].add(cat_name)
            else:
                out_data[out_url] = set([cat_name])


    # convert category sets to lists for serialization
    for url, cat_set in out_data.iteritems():
        out_data[url] = list(cat_set)

    with open('./domain_cat_index.json', 'w') as f:
        json.dump(out_data, f, indent=4, sort_keys=True)


if __name__ == "__main__":
    main()
