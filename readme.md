# Foreign Data Wrappers for LI³DS

This repo includes [Multicorn](http://multicorn.org/)-based [Foreign Data
Wrappers](https://www.postgresql.org/docs/current/static/fdwhandler.html)
for exposing LI³DS data as PostgreSQL tables.

## Prerequisites

- python == 2.7
- numpy
- multicorn

### Install under Ubuntu

Install PostgreSQL 9.5 from PGDG repositories and Python

```sh
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo apt-get install wget ca-certificates
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get upgrade
sudo apt-get install python2.7 python2.7-dev python-setuptools python-pip python-numpy postgresql-9.5 postgresql-server-dev-9.5
```

Compile and install Multicorn
```sh
git clone git@github.com:Kozea/Multicorn.git
cd Multicorn
make
sudo make install
```

## Installation

Clone repository and install with:

	sudo pip install .

or install in editable mode (for development):

	sudo pip install -e .

## Testing

Load the pointcloud extension in order to have the pcpatch type available.

```sql
create extension if not exists pointcloud;
```

### Custom EchoPulse format

```sql
drop extension multicorn cascade;
create extension multicorn;

create server echopulseserver foreign data wrapper multicorn
    options (
        wrapper 'fdwli3ds.EchoPulse'
        , directory 'data/echopulse'
    );

-- create foreign table to retrieve the pointcloud schema dynamically
create foreign table myechopulse_schema (
    schema text
)
server echopulseserver
    options (
        metadata 'true'
    );

insert into pointcloud_formats(pcid, srid, schema)
select 1, -1, schema from myechopulse_schema;

create foreign table myechopulse (
    points pcpatch(1)
) server echopulseserver
    options (
        patch_size '400'
        , pcid '1'
    );

select * from myechopulse;
```

### Sbet files

```sql
create server sbetserver foreign data wrapper multicorn
    options (
        wrapper 'fdwli3ds.Sbet'
    );

create foreign table mysbet_schema (
    schema text
)
server sbetserver
 options (
    metadata 'true'
);

insert into pointcloud_formats (pcid, srid, schema)
select 2, 4326, schema from mysbet_schema;

create foreign table mysbet (
    points pcpatch(2)
) server sbetserver
    options (
        sources 'data/sbet/sbet.bin'
        , patch_size '100'
        , pcid '2'
);


select * from mysbet;

```

### ROS bag files

Create server:

```sql
create server rosbagserver foreign data wrapper multicorn
    options (
        wrapper 'fdwli3ds.Rosbag'
        , rosbag 'data/rosbag/session8_section0_1492648601948956966_0.bag'
    );
```

Create foreign table for the `/INS/SbgLogImuData` topic:

```sql
create foreign table rosbag_imu (
    "status" smallint
    , "temperature" float
    , "timeStamp" integer
    , "accelerometers" float[3]
    , "topic" text
    , "deltaAngle" float[3]
    , "time" bigint
    , "deltaVelocity" float[3]
    , "gyroscopes" float[3]
) server rosbagserver
    options (
        topic '/INS/SbgLogImuData'
);

select * from rosbag_imu limit 20;
```

Create foreign table for the `/Laser/velodyne_points` topic:

```sql
create foreign table rosbag_pointcloud2_format (
    schema text
) server rosbagserver
    options (
        topic '/Laser/velodyne_points'
        , metadata 'true'
);

insert into pointcloud_formats (pcid, srid, schema)
select 3, 4326, schema from rosbag_pointcloud2_format;

create foreign table rosbag_pointcloud2 (
    patch pcpatch(3)
    , ply bytea
    , width int
    , height int
) server rosbagserver
    options (
        topic '/Laser/velodyne_points'
        , pcid '3'
        , max_count '10000'
);


select sum(width*height) from rosbag_pointcloud2;
select sum(pc_numpoints(patch)) from rosbag_pointcloud2;
select pc_get(pc_patchmin(patch)), pc_get(pc_patchmax(patch)) from rosbag_pointcloud2 limit 20;
select encode(ply::varchar(700)::bytea, 'escape') from rosbag_pointcloud2 limit 1;
```

## Unit tests

Pytest is required to launch unit tests.

```
sudo apt-get install python-pytest
```

Or

```bash
pip install -e .[dev]
```

Launch tests:

```bash
py.test
```

## Known Issues

SIGSEGV crashes with PostgreSQL 9.6

If the `postgresql` process crashes with a SIGSEGV error it means that you are hitting the xxHash symbols conflict issue reported in https://github.com/ros/ros_comm/pull/1065. To fix the issue the `roslz4` Python package should be built and installed from source.

```bash
git clone https://github.com/ros/ros_comm
cd utilities/roslz4
mkdir build
cmake -DCMAKE_INSTALL_PREFIX=/usr ..
make
sudo make install
```

## Experimental

Create tables automatically for all topics of a rosbag file, using `IMPORT
SCHEMA`.

```sql
create server rosbagserver
foreign data wrapper multicorn
    options (
      wrapper 'fdwli3ds.Rosbag'
      , rosbag_path 'data/rosbag/'
);

create schema rosbag;

import foreign schema "session8_section0_1492648601948956966_0.bag"
from server rosbagserver into rosbag;

insert into pointcloud_formats
select 4 as pcid, 4326 as srid, schema
from rosbag."/Laser/velodyne_points"
limit 1;
```
