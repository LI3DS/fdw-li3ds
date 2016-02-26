# Foreign Data Wrapper for reading lidar data

Tool to access some lidar data from sql.

## Prerequisites

- python 3
- multicorn
- pip

### Install under Ubuntu

Install PostgreSQL 9.5 from PGDG repositories and Python 3
```sh
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo apt-get install wget ca-certificates
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get upgrade
sudo apt-get install python3 python3-dev postgresql-9.5 postgresql-server-dev-9.5
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

	sudo pip3 install

or installation in editable mode (for development):

	sudo pip3 install -e .

## testing

### Custom EchoPulse format

```sql
drop extension multicorn cascade;
create extension multicorn;

create server echopulse foreign data wrapper multicorn
    options (
        wrapper 'fdwlidar.EchoPulse'
    );

create foreign table myechopulse (
    r float
    , theta float
    , time float
) server echopulse
    options (
        raw 'data/pulse-float32-phi/43724.bin'
        , theta 'data/pulse-float32-theta/43724.bin'
        , time 'data/pulse-linear-time/43724.txt'
);

select * from myechopulse;
```

### Sbet files

Pointcloud's patch creation on the fly example
pgpoincloud extension is needed:

```sql
create extension if not exists pointcloud;
```

Load sbet schema into pointcloud_formats, an example is given in fdw/data/sbetschema.sql

```sql
create server sbetserver foreign data wrapper multicorn
    options (
        wrapper 'fdwlidar.Sbet'
    );

create foreign table mysbet (
    points pcpatch(1)
) server sbetserver
    options (
        filename 'data/sbet.bin'
        , patch_size '100'
);

select * from mysbet;

```
