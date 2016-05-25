# es-reindex

[![Build Status](https://travis-ci.org/MailOnline/es-reindex.svg)](https://travis-ci.org/MailOnline/es-reindex)

CLI tool to reindex all documents from one ES mapping to another, possibly between different ES clusters.

# Installation

Install Haskell Stack. Type `stack install` to install the tool.

# Usage

Examples:

    es-reindex article-v5 article article-v6 article -s http://10.251.64.51:9200/ -d http://10.251.64.51:9200/ -l 100

With a filter (reindexing all articles modified during the previous year):

    echo 'RangeFilter (FieldName "modifiedDate") (RangePair [("gte", String "now-2y/y"), ("lt", String "now-1y/y")]) RangeExecutionIndex False' | es-reindex article-v5 article article-v6 article -s http://10.251.64.51:9200/ -d http://10.251.64.51:9200/ -l 100 -f -

Filters use Haskell syntax for Bloodhound filters (http://hackage.haskell.org/package/bloodhound-0.11.0.0/docs/Database-Bloodhound-Types.html#t:Filter)
