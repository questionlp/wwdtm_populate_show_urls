# Wait Wait Don't Tell Me! Stats Populate Show URLs

## Overview

Python script that populates the new `showurl` copies existing panelist scores into a new decimal table column.

## Requirements

- Python 3.10 or newer
- MySQL Server 8.0 or newer, or another MySQL Server distribution based on MySQL Server 8.0 or newer, hosting a version of the aforementioned Wait Wait Don't Tell Me! Stats database

### Command-Line Flags and Options

There are several flags and options that can be set through the command line:

| Flag/Option | Description |
|---------------|-------------|
| `-f`, `--file` | CSV file with show date and show URLs, including a header. |
| `-b`, `--backfill` | Fill in other show URLs for shows without an existing URL. |

## License

This library is licensed under the terms of the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Original version of the Apache License 2.0 can also be found at: <http://www.apache.org/licenses/LICENSE-2.0>.
