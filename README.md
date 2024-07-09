# SciPy2024

This repository contains scripts used for demonstrations in the talk [No-Code-Change GPU Acceleration for Your Pandas and NetworkX Workflows](https://cfp.scipy.org/2024/talk/KTZHZM/) at SciPy 2024.

This demonstration uses publicly available [Wikipedia database downloads](https://en.wikipedia.org/wiki/Wikipedia:Database_download).
The data was originally taken from the [20240620](https://dumps.wikimedia.org/enwiki/20240620/) dump.
The scripts require two files:
1. The multistream pages dump: `wget https://dumps.wikimedia.org/enwiki/20240620/enwiki-20240620-pages-articles-multistream.xml.bz2`
2. The stub metadata file containing the full revision history: `wget https://dumps.wikimedia.org/enwiki/20240620/enwiki-20240620-stub-meta-history.xml.gz`

Parsing these raw XML files in pure Python is very slow, so parsers were written in Rust.
See the `wikipedia2csv/` subdirectory for a crate that can parse these files (the CLI is self-documenting).
The resulting outputs can then be used to run the demo scripts in the `demos/` directory.


## Licensing

This repository is shared under the same CC-BY 4.0 license as the Wikipedia data, as per [the Wikipedia license](https://en.wikipedia.org/wiki/Wikipedia:Text_of_the_Creative_Commons_Attribution-ShareAlike_4.0_International_License).
Copyright (c) 2024, NVIDIA CORPORATION.
