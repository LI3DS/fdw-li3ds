# Foreign Data Wrapper for reading lidar data

Tool to access some lidar data from sql.

requirements:

- python 3.5
- multicorn (for python3)
- pip

## installation

	pip install

or installation in editable mode (for development):

	pip install -e .


## testing


    create extension multicorn;

	create server echopulse foreign data wrapper multicorn
        options (
            wrapper 'fdwlidar.echopulse'
        );

    create foreign table echopulse (
        r float,
        theta float,
        time float
    ) server echopulse;

    select * from echopulse;

