from sys import byteorder
from struct import pack, unpack, calcsize
from binascii import hexlify

from multicorn import ForeignDataWrapper, ColumnDefinition, TableDefinition
from multicorn.utils import log_to_postgres, WARNING


struct_fmt_dict = {
    'int8': 'b',
    'uint8': 'B',
    'int16': 'h',
    'uint16': 'H',
    'int32': 'i',
    'uint32': 'I',
    'float32': 'f',
    'float64': 'd',
    'int64': 'q',
    'uint64': 'Q',
}


def struct_fmt(typ, array):
    if array == '[]':
        return None
    elems = array[1:-1] if array else ''
    if typ not in struct_fmt_dict:
        return None
    return elems + struct_fmt_dict[typ]


def get_column_def(col, typ, array, typmod, fmt):
    py2sql = {
        'int8': 'int2',
        'uint8': 'int2',
        'int16': 'int2',
        'uint16': 'int4',
        'int32': 'int4',
        'uint32': 'int8',
        'float32': 'float4',
        'float64': 'float8',
        'int64': 'int8',
        'uint64': 'int8',
        'bool': 'bool',
        'string': 'text',
    }
    if typ in py2sql:
        typ = py2sql[typ]
    typ += array
    if typmod:
        typ = "{}({})".format(typ, typmod)
    return ColumnDefinition(col, type_name=typ)


def get_schema_and_fmt(patch_columns, columns):
    pyschema = []
    fmt = '='
    s = "bBhHiIfdqQ"
    for column in patch_columns:
        f = columns[column][3]
        fmt += f
        datatype = s.index(f[-1])
        f = f[:-1]
        if f:
            pyschema.extend(((datatype, "{}[{}]".format(column, i)) for i in range(int(f))))
        else:
            pyschema.append((datatype, column))
    schema = get_schema(pyschema)
    return schema, fmt


def get_point_data(row, columns, fmt):
    val = list()
    for column in columns:
        if isinstance(row[column], tuple):
            val.extend(row[column])
        else:
            val.append(row[column])
    return pack(fmt, *val)


def get_fields_with_extra_bytes(msg):
    fields = sorted(msg.fields, key=lambda f: f.offset)
    sizes = [0, 1, 1, 2, 2, 4, 4, 4, 8]
    offset = 0
    for field in fields:
        assert(field.count == 1)
        assert(field.offset >= offset)
        while offset < field.offset:
            yield 1, 'extra_byte_{}'.format(offset)
            offset += 1
        yield field.datatype-1, field.name
        offset += sizes[field.datatype]
    while offset < msg.point_step:
        yield 1, 'extra_byte_{}'.format(offset)
        offset += 1


def get_ply_header(fields):
    header = ("ply\n"
              "format binary_{endianness}_endian 1.0\n"
              "comment filename {filename}\n"
              "comment topic {topic}\n"
              "element vertex {count}\n")
    interp = [
        'int8', 'uint8', 'int16', 'uint16',
        'int32', 'uint32', 'float32', 'float64'
    ]
    for datatype, name in fields:
        header += 'property {} {}\n'.format(interp[datatype], name)
    return header + 'end_header\n'


def get_schema(pyschema):
    if not pyschema:
        return None
    interps = [
        'int8_t', 'uint8_t', 'int16_t', 'uint16_t',
        'int32_t', 'uint32_t', 'float', 'double', 'int64_t', 'uint64_t'
    ]
    sizes = [1, 1, 2, 2, 4, 4, 4, 8, 8, 8]
    schema = \
        '<?xml version="1.0" encoding="UTF-8"?>\n<pc:PointCloudSchema' \
        ' xmlns:pc="http://pointcloud.org/schemas/PC/1.1"\n   ' \
        ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
    for i, (datatype, name) in enumerate(pyschema):
        schema += \
            '  <pc:dimension>\n' \
            '    <pc:position>{}</pc:position>\n' \
            '    <pc:size>{}</pc:size>\n' \
            '    <pc:description>{}</pc:description>\n' \
            '    <pc:name>{}</pc:name>\n' \
            '    <pc:interpretation>{}</pc:interpretation>\n' \
            '  </pc:dimension>\n'.format(
                i+1, sizes[datatype], name, name, interps[datatype])
    return schema + '</pc:PointCloudSchema>\n'


def get_columns(bag, topic, infos, pcid, patch_column, patch_columns):
    # read the first message to introspect its type
    # internal connection api could have been used instead
    _, msg, _ = next(bag.read_messages(topics=topic))

    res = list(get_columns_from_message(msg))
    patch_schema = None
    patch_ply_header = None
    endianness = 0
    res.append(("filename", ("string", "", 0, '')))
    res.append(("topic", ("string", "", 0, '')))
    res.append(("time", ("uint64", "", 0, 'Q')))
    if infos.msg_type == 'sensor_msgs/PointCloud2':
        patch_columns = []
        res.append((patch_column, ("pcpatch", "", pcid, '')))
        res.append(("ply", ("bytea", "", 0, '')))
        fields = list(get_fields_with_extra_bytes(msg))
        patch_schema = get_schema(fields)
        patch_ply_header = get_ply_header(fields)
        endianness = 0 if msg.is_bigendian else 1

    elif patch_column:
        # wildcard '*' selects, in sorted order, all numeric fields
        if '*' in patch_columns:
            patch_columns = sorted([c[0] for c in res if c[1][0] in struct_fmt_dict.keys()])
        patch_schema, patch_fmt = get_schema_and_fmt(patch_columns, dict(res))
        res.append((patch_column, ("pcpatch", "", pcid, patch_fmt)))
        endianness = 0 if byteorder is 'big' else 1
        res = [(k, v) for (k, v) in res if k not in patch_columns]

    has_lon = 'longitude' in patch_columns or 'lon' in patch_columns
    has_lat = 'latitude' in patch_columns or 'lat' in patch_columns
    patch_srid = 4326 if (has_lon and has_lat) else 0
    return dict(res), patch_schema, patch_ply_header, endianness, patch_columns, patch_srid


def get_columns_from_message(msg, cols=[], typ_suffix=""):
    if len(msg.__slots__) == 1:
        attr = object.__getattribute__(msg, msg.__slots__[0])
        if isinstance(attr, list):
            msg = attr[0]
    terminal_types = {
        'time': ('uint64', '', 0, 'Q'),
        'geometry_msgs/Quaternion': ('float64', '[4]', 0, '4d'),
        'geometry_msgs/Vector3': ('float64', '[3]', 0, '3d'),
    }
    for col, typ in zip(msg.__slots__, msg._slot_types):
        subcols = list(cols)
        subcols.append(col)
        if typ in terminal_types:
            yield (".".join(subcols), terminal_types[typ])
            continue
        pos_array = typ.find('[')
        subtyp_suffix = typ_suffix
        if pos_array > 0:
            subtyp_suffix += typ[pos_array:]
            typ = typ[:pos_array]
        attr = object.__getattribute__(msg, col)
        count = 1
        if hasattr(attr, '__len__'):
            count = len(attr)
            attr = attr[0]
        if hasattr(attr, '__slots__'):
            for col in get_columns_from_message(attr, subcols, subtyp_suffix):
                yield col
        else:
            if subtyp_suffix == '[]':
                if typ == 'uint8':
                    typ, subtyp_suffix = 'bytea', ''
                else:
                    subtyp_suffix = '[{}]'.format(count)
            yield (".".join(subcols), (typ, subtyp_suffix, 0, struct_fmt(typ, subtyp_suffix)))


def import_bag(options):
    import sys
    python_path = options.pop('python_path', None)
    if python_path:
        sys.path.extend(python_path.split(','))
    if not hasattr(sys, 'argv'):
        sys.argv = ['']
    from rosbag import Bag
    return Bag


class Rosbag(ForeignDataWrapper):
    def __init__(self, options, columns=None):
        super(Rosbag, self).__init__(options, columns)
        Bag = import_bag(options)
        self.filename = options.pop('rosbag_path', "") + options.pop('rosbag')
        self.topic = options.pop('topic', None)
        pointcloud_formats = options.pop('metadata', 'false') == 'true'

        self.patch_column = options.pop('patch_column', 'patch').strip()
        self.patch_columns = options.pop('patch_columns', '*').strip()
        self.patch_columns = [col.strip() for col in self.patch_columns.split(',') if col.strip()]

        self.patch_count_default = int(options.pop('patch_count_default', 1000))
        # 0 => 1 patch per message
        self.patch_count_pointcloud = int(options.pop('patch_count_pointcloud', 0))
        assert(self.patch_count_default > 0)
        assert(self.patch_count_pointcloud >= 0)
        self.pcid = int(options.pop('pcid', 0))
        self.bag = Bag(self.filename, 'r')
        self.topics = self.bag.get_type_and_topic_info().topics
        self.pointcloud_formats = None
        if pointcloud_formats:
            self.pointcloud_formats = []
            topics = self.topic.split(',') if self.topic else self.topics
            for i, topic in enumerate(self.topics):
                if topic not in topics:
                    continue
                infos = self.topics[topic]
                columns, patch_schema, patch_ply_header, _, patch_columns, patch_srid = \
                    get_columns(self.bag, topic, infos, self.pcid+i+1, self.patch_column,
                                self.patch_columns)
                self.pointcloud_formats.append({
                    'pcid': self.pcid+i+1,
                    'srid': patch_srid,
                    'schema': patch_schema,
                    'format': columns[self.patch_column][3],
                    'rostype': infos.msg_type,
                    'columns': patch_columns,
                    'ply_header': patch_ply_header,
                })
            return

        self.pcid += 1 + self.topics.keys().index(self.topic)
        self.infos = self.topics[self.topic]
        (self.columns, self.patch_schema, self.patch_ply_header, self.endianness,
         self.patch_columns, self.patch_srid) = \
            get_columns(self.bag, self.topic, self.infos, self.pcid, self.patch_column,
                        self.patch_columns)

        if columns:
            missing = set(columns) - set(self.columns.keys())
            columns = list(c for c in self.columns.keys() if c in columns)
            if missing:
                missing = ", ".join(sorted(missing))
                support = ", ".join(sorted(self.columns.keys()))
                log_to_postgres(
                    "extra unsupported columns : {}".format(missing), WARNING,
                    hint="supported columns : {}".format(support))
            self.columns = {col: self.columns[col] for col in columns}

        if options:
            log_to_postgres("extra unsupported options : {}".format(
                options.keys()), WARNING)

    @classmethod
    def import_schema(self, schema, srv_options, options,
                      restriction_type, restricts):
        Bag = import_bag(srv_options)
        pcid_str = options.pop('pcid', srv_options.pop('pcid', 0))
        pcid = int(pcid_str)
        patch_column = options.pop('patch_column', srv_options.pop('patch_column', 'patch'))
        patch_columns = options.pop('patch_columns', '*').strip()
        patch_columns = [col.strip() for col in patch_columns.split(',') if col.strip()]
        filename = srv_options.pop('rosbag_path', "") + options.pop('rosbag_path', "") + schema
        bag = Bag(filename, 'r')

        tablecols = []
        topics = bag.get_type_and_topic_info().topics
        pcid_for_topic = {k: pcid+1+i for i, k in enumerate(topics.keys())}
        pointcloud_formats = True
        if restriction_type is 'limit':
            topics = {k: v for k, v in topics.items() if k in restricts}
            pointcloud_formats = 'pointcloud_formats' in restricts
        elif restriction_type is 'except':
            topics = {k: v for k, v in topics.items() if k not in restricts}
            pointcloud_formats = 'pointcloud_formats' not in restricts

        tabledefs = []
        if pointcloud_formats:
            tablecols = [
                ColumnDefinition('pcid', type_name='integer'),
                ColumnDefinition('srid', type_name='integer'),
                ColumnDefinition('schema', type_name='text'),
                ColumnDefinition('format', type_name='text'),
                ColumnDefinition('rostype', type_name='text'),
                ColumnDefinition('columns', type_name='text[]'),
                ColumnDefinition('ply_header', type_name='text'),
            ]
            tableopts = {'metadata': 'true', 'rosbag': schema, 'pcid': pcid_str}
            tabledefs.append(TableDefinition("pointcloud_formats", columns=tablecols,
                                             options=tableopts))

        for topic, infos in topics.items():
            columns, _, _, _, _, _ = get_columns(bag, topic, infos, pcid_for_topic[topic],
                                                 patch_column, patch_columns)
            tablecols = [get_column_def(k, *v) for k, v in columns.items()]
            tableopts = {'topic': topic, 'rosbag': schema, 'pcid': pcid_str}
            tabledefs.append(TableDefinition(topic, columns=tablecols, options=tableopts))
        return tabledefs

    def execute(self, quals, columns):
        if self.pointcloud_formats is not None:
            for f in self.pointcloud_formats:
                yield f
            return
        self.patch_data = ''
        from rospy.rostime import Time
        tmin = None
        tmax = None
        for qual in quals:
            if qual.field_name == "time":
                t = int(qual.value)
                t = Time(t / 1000000000, t % 1000000000)
                if qual.operator in ['=', '>', '>=']:
                    tmin = t
                if qual.operator in ['=', '<', '<=']:
                    tmax = t
        for topic, msg, t in self.bag.read_messages(
                topics=self.topic, start_time=tmin, end_time=tmax):
            for row in self.get_rows(topic, msg, t, columns):
                yield row

        # flush leftover patch data
        if self.patch_data and self.last_row:
            count = int((len(self.patch_data) / self.point_size))
            # in replicating mode, a single leftover point must not be reported
            if count > 1 or self.patch_step_size == self.patch_size:
                res = self.last_row
                if self.patch_column in columns:
                    res[self.patch_column] = hexlify(
                            pack('=b3I', self.endianness, self.pcid, 0, count) + self.patch_data)
                if self.patch_ply_header and 'ply' in columns:
                    self.ply_info['count'] = count
                    res['ply'] = self.patch_ply_header.format(**self.ply_info) + self.patch_data
                yield res

    def get_rows(self, topic, msg, t, columns, toplevel=True):
        if toplevel and len(msg.__slots__) == 1:
            attr = object.__getattribute__(msg, msg.__slots__[0])
            if isinstance(attr, list):
                for msg in attr:
                    for row in self.get_rows(topic, msg, t, columns, False):
                        yield row
                return
        res = {}
        data_columns = set(columns)
        if self.patch_column in columns:
            data_columns = data_columns.union(self.patch_columns) - set([self.patch_column])
        if "filename" in data_columns:
            res["filename"] = self.filename
        if "topic" in data_columns:
            res["topic"] = topic
        if "time" in data_columns:
            res["time"] = t.to_nsec()
        if self.infos.msg_type == 'sensor_msgs/PointCloud2':
            self.patch_count = self.patch_count_pointcloud or (msg.width*msg.height)
            self.point_size = msg.point_step
            self.patch_size = self.patch_count * self.point_size
            self.patch_step_size = self.patch_size
            self.endianness = 0 if msg.is_bigendian else 1
            data_columns = data_columns - set(['ply', self.patch_column])
            self.patch_data += msg.data

        data_columns = data_columns - set(res.keys())
        for column in data_columns:
            attr = msg
            for col in column.split('.'):
                if isinstance(attr, list):
                    attr = tuple(object.__getattribute__(a, col) for a in attr)
                else:
                    attr = object.__getattribute__(attr, col)
            if hasattr(attr, "to_nsec"):
                attr = attr.to_nsec()
            elif hasattr(attr, "x"):
                if hasattr(attr, "w"):
                    attr = (attr.x, attr.y, attr.z, attr.w)
                else:
                    attr = (attr.x, attr.y, attr.z)
            elif isinstance(attr, str):
                fmt = self.columns[column][3]
                if fmt:
                    attr = unpack(fmt, attr)
            res[column] = attr

        if self.patch_column in columns and not self.infos.msg_type == 'sensor_msgs/PointCloud2':
            fmt = self.columns[self.patch_column][3]
            self.patch_count = self.patch_count_default
            self.point_size = calcsize(fmt)
            self.patch_size = self.patch_count * self.point_size
            self.patch_step_size = self.patch_size - self.point_size
            self.patch_data += get_point_data(res, self.patch_columns, fmt)
            res = {k: v for k, v in res.items() if k not in self.patch_columns}

        if not self.patch_data:
            yield res
        else:
            # todo: ensure current res and previous res are equal if there is some leftover
            # patch_data
            while len(self.patch_data) >= self.patch_size:
                data = self.patch_data[0:self.patch_size]
                count = int(self.patch_size / self.point_size)
                res[self.patch_column] = hexlify(
                        pack('=b3I', self.endianness, self.pcid, 0, count) + data)
                if self.patch_ply_header and 'ply' in columns:
                    self.ply_info = {
                        'endianness': 'big' if self.endianness else 'little',
                        'filename': self.filename,
                        'topic': self.topic,
                        'count': count
                    }
                    res['ply'] = self.patch_ply_header.format(**self.ply_info) + data
                self.patch_data = self.patch_data[self.patch_step_size:]
                yield res
            self.last_row = res
