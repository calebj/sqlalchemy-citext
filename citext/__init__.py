from __future__ import unicode_literals

import psycopg2.extensions
import sqlalchemy
import sqlalchemy.event as event
import sqlalchemy.types as types
from sqlalchemy.dialects.postgresql.base import ischema_names


__version__ = '1.9.0'


class CIText(types.Concatenable, types.UserDefinedType):
    cache_ok = True

    # This is copied from the `literal_processor` of sqlalchemy's own `String`
    # type.
    def literal_processor(self, dialect):
        def process(value):
            value = value.replace("'", "''")

            if dialect.identifier_preparer._double_percents:
                value = value.replace("%", "%%")

            return "'%s'" % value

        return process

    def get_col_spec(self):
        return 'CITEXT'

    def bind_processor(self, dialect):
        def process(value):
            return value
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value
        return process


# Register CIText to SQLAlchemy's Postgres reflection subsystem.
ischema_names['citext'] = CIText


def _dbapi2_get_array_oid(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT typarray FROM pg_type WHERE typname = 'citext'")
        res = cur.fetchone()

    if res:
        return res[0]


def register_citext_array(engine: sqlalchemy.engine.Engine):
    driver = engine.url.get_driver_name()

    if driver in ('psycopg2', 'psycopg2cffi'):
        if driver == 'psycopg2':
            import psycopg2
        else:
            import psycopg2cffi as psycopg2

        def connect(dbapi_connection, connection_record):
            res = _dbapi2_get_array_oid(dbapi_connection)

            if res:
                array_type = psycopg2.extensions.new_array_type((res,), 'citext[]', psycopg2.STRING)
                psycopg2.extensions.register_type(array_type, dbapi_connection)
    elif driver == 'psycopg':
        import psycopg

        def connect(dbapi_connection, connection_record):
            res = psycopg.types.TypeInfo.fetch(dbapi_connection, 'citext')
            if res:
                res.register(dbapi_connection)
    elif driver == 'pg8000':
        from pg8000.converters import string_array_in

        def connect(dbapi_connection, connection_record):
            res = _dbapi2_get_array_oid(dbapi_connection)

            if res:
                dbapi_connection.register_in_adapter(res, string_array_in)
    elif driver == 'asyncpg':
        return  # asyncpg handles this already
    else:
        raise RuntimeError("Unknown driver: " + driver)

    event.listens_for(engine, "connect")(connect)


if __name__ == '__main__':
    from sqlalchemy import create_engine, MetaData, Integer, ARRAY, __version__ as sa_version
    from sqlalchemy.schema import Column
    import sqlalchemy.orm as orm

    # declarative_base was moved to orm in 1.4
    if sa_version.split('.') < '1.4'.split('.'):
        from sqlalchemy.ext.declarative import declarative_base
    else:
        from sqlalchemy.orm import declarative_base

    engine = create_engine('postgresql://localhost/test_db')
    register_citext_array(engine)
    meta = MetaData()
    Base = declarative_base(metadata=meta)
    conn = engine.connect()


    class TestObj(Base):
        __tablename__ = 'test'
        id = Column(Integer, primary_key=True)
        txt = Column(CIText)
        txt_array = Column(ARRAY(CIText))

        def __repr__(self):
            return "TestObj(%r, %r, %r)" % (self.id, self.txt, self.txt_array)


    with conn.begin():
        meta.drop_all(bind=conn)
        meta.create_all(bind=conn)

    Session = orm.sessionmaker(bind=engine)
    ses = Session()

    to = TestObj(id=1, txt='FooFighter', txt_array=['foo', 'bar'])
    ses.add(to)
    ses.commit()
    row = ses.query(TestObj).filter(TestObj.txt == 'foofighter').all()
    assert len(row) == 1
    assert row[0].txt_array == ['foo', 'bar']
    print(row)
    ses.close()
