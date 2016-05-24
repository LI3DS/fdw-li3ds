drop extension multicorn cascade;
create extension multicorn;

create server echopulse foreign data wrapper multicorn
    options (
        wrapper 'fdwpointcloud.echopulse.EchoPulse'
        , raw 'data/pulse-float32-phi/43724.bin'
        , theta 'data/pulse-float32-theta/43724.bin'
        , time 'data/pulse-linear-time/43724.txt'
    );

create foreign table myechopulse (
    r float
    , theta float
    , time float
) server echopulse;

select * from myechopulse;
