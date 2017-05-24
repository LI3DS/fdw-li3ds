from multicorn import ForeignDataWrapper, ColumnDefinition, TableDefinition
from multicorn.utils import log_to_postgres, WARNING


from struct import pack, unpack
from binascii import hexlify


def unpack_fmt(typ, array):
    if not array or array == '[]':
        return None
    fmt = {
        'int8': 'b',
        'int16': 'h',
        'int32': 'i',
        'int64': 'l',
        'uint8': 'B',
        'uint16': 'H',
        'uint32': 'I',
        'uint64': 'L',
        'float32': 'f',
        'float64': 'd',
    }
    if typ not in fmt:
        return None
    return fmt[typ] * int(array[1:-1])


def sql_fmt(col, typ, array, typmod):
    py2sql = {
        'int8': 'int2',
        'int16': 'int2',
        'int32': 'int4',
        'int64': 'int8',
        'uint8': 'int2',
        'uint16': 'int4',
        'uint32': 'int8',
        'uint64': 'int8',
        'float32': 'float4',
        'float64': 'float8',
        'bool': 'bool',
        'string': 'text',
    }
    if typ in py2sql:
        typ = py2sql[typ]
    typ += array
    if typmod:
        typ = "{}({})".format(typ, typmod)
    return ColumnDefinition(col, type_name=typ)


def get_split(data, data_size, point_size, max_count):
    max_size = data_size
    if max_count:
        max_size = max_count * point_size
    offset = 0
    while offset < data_size:
        size = min(max_size, data_size - offset)
        count = size / point_size
        yield offset, count, data[offset:offset + size]
        offset += size


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
        self.max_count = int(options.pop('max_count', 0))
        self.pcid = int(options.pop('pcid', 1))
        self.bag = Bag(self.filename, 'r')
        self.topics = self.bag.get_type_and_topic_info().topics
        self.infos = self.topics[self.topic]
        self.columns = self.get_columns(self.bag, self.topic,
                                        self.infos, self.pcid)
        if columns:
            cols = set(c[0] for c in self.columns)
            missing = set(columns) - cols
            columns = list(c for c in self.columns if c[0] in columns)
            if missing:
                missing = ", ".join(sorted(missing))
                support = ", ".join(sorted(cols))
                log_to_postgres(
                    "extra unsupported columns : {}".format(missing), WARNING,
                    hint="supported columns : {}".format(support))
            self.columns = columns
        self.unpack_fmt = {k: unpack_fmt(u, v) for k, u, v, _ in self.columns}
        self.columns = {k: sql_fmt(k, u, v, w) for k, u, v, w in self.columns}
        if options:
            log_to_postgres("extra unsupported options : {}".format(
                options.keys()), WARNING)

    @classmethod
    def import_schema(cls, schema, srv_options, options,
                      restriction_type, restricts):
        Bag = import_bag(srv_options)
        pcid = int(srv_options.pop('pcid', 1))
        filename = srv_options.pop('rosbag_path', "") + schema
        bag = Bag(filename, 'r')

        topics = bag.get_type_and_topic_info().topics
        if restriction_type is 'limit':
            topics = {k: v for k, v in topics.items() if k in restricts}
        elif restriction_type is 'except':
            topics = {k: v for k, v in topics.items() if k not in restricts}

        res = []
        for topic, infos in topics.items():
            columns = cls.get_columns(bag, topic, infos, pcid)
            tablecols = [sql_fmt(*col) for col in columns]
            tableopts = {'topic': topic, 'rosbag': schema}
            res.append(TableDefinition(topic, columns=tablecols,
                                       options=tableopts))
        return res

    def execute(self, quals, columns):
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

    @classmethod
    def get_columns(cls, bag, topic, infos, pcid):
        # read the first message to introspect its type
        # internal connection api could have been used instead
        _, msg, _ = next(bag.read_messages(topics=topic))
        res = cls.get_columns_from_message(msg)
        res.append(("topic", "string", "", 0))
        res.append(("time", "uint64", "", 0))
        if infos.msg_type == 'sensor_msgs/PointCloud2':
            res.append(("schema", "string", "", 0))
            res.append(("ply", "bytea", "", 0))
            res.append(("patch", "pcpatch", "", pcid))
        return res

    @classmethod
    def get_columns_from_message(cls, msg, cols=[], typ_suffix=""):
        res = []
        for col, typ in zip(msg.__slots__, msg._slot_types):
            pos_array = typ.find('[')
            subtyp_suffix = typ_suffix
            subcols = list(cols)
            subcols.append(col)
            if pos_array > 0:
                subtyp_suffix += typ[pos_array:]
                typ = typ[:pos_array]
            attr = object.__getattribute__(msg, col)
            if isinstance(attr, list):
                attr = attr[0]
            if hasattr(attr, '__slots__'):
                columns = cls.get_columns_from_message(
                    attr, subcols, subtyp_suffix)
                res.extend(columns)
            else:
                if typ == 'uint8' and subtyp_suffix == '[]':
                    typ, subtyp_suffix = 'bytea', ''
                res.append((".".join(subcols), typ, subtyp_suffix, 0))
        return res

    @staticmethod
    def get_fields_with_extra_bytes(msg):
        fields = sorted(msg.fields, key=lambda f: f.offset)
        offset = 0
        sizes = [0, 1, 1, 2, 2, 4, 4, 4, 8]
        for field in fields:
            assert(field.count == 1)
            assert(field.offset >= offset)
            while offset < field.offset:
                yield 1, 'extra_byte_{}'.format(offset), 1
                offset += 1
            size = sizes[field.datatype]
            yield field.datatype-1, field.name, size
            offset += size
        while offset < msg.point_step:
            yield 1, 'extra_byte_{}'.format(offset), 1
            offset += 1

    def get_plys(self, msg):
        interp = [
            'int8', 'uint8', 'int16', 'uint16',
            'int32', 'uint32', 'float32', 'float64'
        ]
        endianness = "big" if msg.is_bigendian else "little"
        header = 'ply\nformat binary_{}_endian 1.0\ncomment file {}\n' \
                 'comment topic {}\ncomment seq {}\n'.format(
                    endianness, self.filename, self.topic, msg.header.seq)
        header += 'comment offset {}\ncomment vertex0 {}\nelement vertex {}\n'
        for datatype, name, _ in \
                self.__class___.get_fields_with_extra_bytes(msg):
            header += 'property {} {}\n'.format(interp[datatype], name)
        header += 'end_header\n'
        for offset, count, data in get_split(msg.data, msg.row_step,
                                             msg.point_step, self.max_count):
            h = header.format(offset, offset / msg.point_step, count)
            yield h + data

    def get_patches(self, msg):
        endianness = 0 if msg.is_bigendian else 1
        fmt = ['>b3I', '<b3I'][endianness]
        for offset, count, data in get_split(msg.data, msg.row_step,
                                             msg.point_step, self.max_count):
            header = pack(fmt, endianness, self.pcid, 0, count)
            yield hexlify(header + data)

    @classmethod
    def get_schema(cls, msg):
        interp = [
            'int8_t', 'uint8_t', 'int16_t', 'uint16_t',
            'int32_t', 'uint32_t', 'float32_t', 'float64_t'
        ]
        schema = \
            '<?xml version="1.0" encoding="UTF-8"?>\n<pc:PointCloudSchema' \
            ' xmlns:pc="http://pointcloud.org/schemas/PC/1.1"\n   ' \
            ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
        for i, (datatype, name, size) in enumerate(
                cls.get_fields_with_extra_bytes(msg)):
            schema += \
                '  <pc:dimension>\n' \
                '    <pc:position>{}</pc:position>\n' \
                '    <pc:size>{}</pc:size>\n' \
                '    <pc:description>{}</pc:description>\n' \
                '    <pc:name>{}</pc:name>\n' \
                '    <pc:interpretation>{}</pc:interpretation>\n' \
                '  </pc:dimension>\n'.format(
                    i+1, size, name, name, interp[datatype])
        return schema + '</pc:PointCloudSchema>\n'

    def get_rows(self, topic, msg, t, columns):
        res = {}
        gen = {}
        if "topic" in columns:
            res["topic"] = topic
        if "time" in columns:
            res["time"] = t.to_nsec()
        if self.infos.msg_type == 'sensor_msgs/PointCloud2':
            if "patch" in columns:
                gen["patch"] = self.get_patches(msg)
            if "schema" in columns:
                res["schema"] = self.get_schema(msg)
            if "ply" in columns:
                gen["ply"] = self.get_plys(msg)

        columns = set(columns) - set(res.keys() + gen.keys())
        for column in columns:
            attr = msg
            for col in column.split('.'):
                if isinstance(attr, list):
                    attr = tuple(object.__getattribute__(a, col) for a in attr)
                else:
                    attr = object.__getattribute__(attr, col)
            if isinstance(attr, str):
                fmt = self.unpack_fmt[column]
                if fmt:
                    attr = unpack(fmt, attr)
            res[column] = attr
        if gen:
            try:
                while True:
                    for column, value in gen.items():
                        res[column] = next(value)
                    yield res
            except StopIteration:
                # Check if all gens ended simultanneously
                leftover = [column for column, value in gen.items()
                            if next(value, None)]
                if leftover:
                    log_to_postgres("leftover values in {}".format(leftover),
                                    WARNING)
        else:
            yield res
