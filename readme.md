# Foreign Data Wrapper for pointcloud data

## Prerequisites

- python >= 3.4
- numpy
- multicorn

### Install under Ubuntu

Install PostgreSQL 9.5 from PGDG repositories and Python 3
```sh
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo apt-get install wget ca-certificates
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get upgrade
sudo apt-get install python3 python3-dev python3-setuptools python3-pip postgresql-9.5 postgresql-server-dev-9.5 python3-numpy
```

Compile and install Multicorn
```sh
git clone git@github.com:Kozea/Multicorn.git
cd Multicorn
export PYTHON_OVERRIDE=python3
make
sudo PYTHON_OVERRIDE=python3 make install
```

## Installation

Clone repository and install with:

	sudo pip3 install .

or install in editable mode (for development):

	sudo pip3 install -e .

## Testing

Load the pointcloud extension in order to have the pcpatch type available.

```sql
create extension if not exists pointcloud;
```

### Custom EchoPulse format

```sql
drop extension multicorn cascade;
create extension multicorn;

create server echopulse foreign data wrapper multicorn
    options (
        wrapper 'fdwpointcloud.EchoPulse'
    );

-- create foreign table to retrieve the pointcloud schema dynamically
create foreign table myechopulse_schema (
    schema text
)
server echopulse
    options (
        directory 'data/echopulse'
        , metadata 'true'
    );

insert into pointcloud_formats(pcid, srid, schema)
select 1, -1, schema from myechopulse_schema;

create foreign table myechopulse (
    points pcpatch(1)
) server echopulse
    options (
        directory 'data/echopulse'
        , patch_size '400'
        , pcid '1'
    );

select * from myechopulse;
```

### Sbet files

```sql
create server sbetserver foreign data wrapper multicorn
    options (
        wrapper 'fdwpointcloud.Sbet'
    );

create foreign table mysbet_schema (
    schema text
)
server route_server
 options (
    metadata 'true'
);

insert into pointcloud_formats (pcid, srid, schema)
select 2, 4326, schema from mysbet_schema;

create foreign table mysbet (
    points pcpatch(2)
) server sbetserver
    options (
        sources 'data/sbet.bin'
        , patch_size '100'
        , pcid '2'
);


select * from mysbet;

```


## Unit tests

Pytest is required to launch unit tests.

```bash
pip install -e .[dev]
```

Launch tests:

```bash
py.test
```

