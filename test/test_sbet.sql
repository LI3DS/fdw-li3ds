-- Drop everything so that we test the latest code
drop extension pointcloud cascade;
create extension if not exists pointcloud;
drop extension multicorn cascade;
create extension if not exists multicorn;

-- Create the server for Sbet files
drop server if exists sbetserver cascade;
create server sbetserver foreign data wrapper multicorn
options (
    wrapper 'fdwpointcloud.Sbet'
);

-- Get the metadata for Sbet
-- Currently static, but could be computed given the file
create foreign table mysbet_schema (
    schema text
) server sbetserver
options (
    metadata 'true'
);

-- Insert the schema into the pointcloud formats table
truncate pointcloud_formats;
insert into pointcloud_formats (pcid, srid, schema)
select 1, 4326, schema from mysbet_schema;

-- The data table
create foreign table mysbet (
    points pcpatch(1)
) server sbetserver
options (
    sources 'data/sbet/*.bin'
    , patch_size '100'
);

-- Let's try it
select pc_astext(points) from mysbet limit 3;
