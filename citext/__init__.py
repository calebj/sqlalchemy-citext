from __future__ import unicode_literals

import psycopg2.extensions
import sqlalchemy
import sqlalchemy.types as types
from sqlalchemy.dialects.postgresql.base import ischema_names


__version__ = '1.8.0'


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


def register_citext_array(engine):
    """Call once with an engine for citext values to be returned as strings instead of characters"""
    results = engine.execute(sqlalchemy.text("SELECT typarray FROM pg_type WHERE typname = 'citext'"))
    oids = tuple(row[0] for row in results)
    array_type = psycopg2.extensions.new_array_type(oids, 'citext[]', psycopg2.STRING)
    psycopg2.extensions.register_type(array_type, None)


if __name__ == '__main__':
    from sqlalchemy import create_engine, MetaData, Integer, __version__ as sa_version
    from sqlalchemy.schema import Column
    import sqlalchemy.orm as orm

    # declarative_base was moved to orm in 1.4
    if sa_version.split('.') < '1.4'.split('.'):
        from sqlalchemy.ext.declarative import declarative_base
    else:
        from sqlalchemy.orm import declarative_base

    engine = create_engine('postgresql://localhost/test_db')
    meta = MetaData()
    Base = declarative_base(metadata=meta)
    conn = engine.connect()


    class TestObj(Base):
        __tablename__ = 'test'
        id = Column(Integer, primary_key=True)
        txt = Column(CIText)

        def __repr__(self):
            return "TestObj(%r, %r)" % (self.id, self.txt)


    with conn.begin():
        meta.drop_all(bind=conn)
        meta.create_all(bind=conn)

    Session = orm.sessionmaker(bind=engine)
    ses = Session()

    to = TestObj(id=1, txt='FooFighter')
    ses.add(to)
    ses.commit()
    row = ses.query(TestObj).filter(TestObj.txt == 'foofighter').all()
    assert len(row) == 1
    print(row)
    ses.close()
