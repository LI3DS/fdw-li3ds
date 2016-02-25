# Foreign Data Wrapper for reading lidar data

Tool to access some lidar data from sql.

requirements:

- python 3.5
- multicorn (for python3)
- pip

## installation

	pip install .

The editable mode could be used for development (add -e to the previous command line)


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

