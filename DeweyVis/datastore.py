from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column, ForeignKey, Integer, String, create_engine)
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime
from contextlib import contextmanager


Base = declarative_base()
conn_string = 'sqlite:///files/datastore.db'


class Bib(Base):
    __tablename__ = 'bib'
    bid = Column(Integer, primary_key=True, autoincrement=False)
    dateCreated = Column(String)
    dateCataloged = Column(String)
    bsid = Column(Integer, ForeignKey('bStatus.bsid'))
    itemCount = Column(Integer)
    callNo = Column(String)
    cid = Column(Integer, ForeignKey('country.cid'))
    pubDate = Column(String)
    laid = Column(Integer, ForeignKey('lang.laid'))
    csid = Column(Integer, ForeignKey('catSource.csid'))
    caid = Column(Integer, ForeignKey('catAgency.caid'))

    items = relationship('Item', cascade='all, delete-orphan')

    def __repr__(self):
        return "<Bib(bid='%s', dateCreated='%s', dateCataloged='%s', " \
            "status='%s', itemCount='%s', callNo='%s', country='%s', " \
            "lang='%s', catSource='%s', catAgency='%s')>" % (
                self.id, self.dateCreated, self.dateCataloged,
                self.status, self.itemCount, self.callNo, self.country,
                self.lang, self.catSource, self.catAgency)


class Bstatus(Base):
    __tablename__ = 'bStatus'
    bsid = Column(Integer, primary_key=True)
    code = Column(String)

    def __repr__(self):
        return "<Bstatus(bsid='%s', bid='%s', code='%s')>" % (
            self.bsid, self.bid, self.code)


class Country(Base):
    __tablename__ = 'country'
    cid = Column(Integer, primary_key=True)
    code = Column(String)

    def __repr__(self):
        return "<Country(cid='%s', bid='%s', code='%s')>" % (
            self.cid, self.bid, self.code)


class Lang(Base):
    __tablename__ = 'lang'
    laid = Column(Integer, primary_key=True)
    code = Column(String)

    def __repr__(self):
        return "<Lang(lid='%s', code='%s')>" % (
            self.lid, self.code)


class CatSource(Base):
    __tablename__ = 'catSource'
    csid = Column(Integer, primary_key=True)
    code = Column(String)

    def __repr__(self):
        return "<CatSource(csid='%s', code='%s')>" % (
            self.csid, self.code)


class CatAgency(Base):
    __tablename__ = 'catAgency'
    caid = Column(Integer, primary_key=True)
    code = Column(String)

    def __repr__(self):
        return "<CatAgency(caid='%s', code='%s')>" % (
            self.caid, self.code)


class Item(Base):
    __tablename__ = 'item'
    iid = Column(Integer, primary_key=True, autoincrement=False)
    bid = Column(Integer, ForeignKey('bib.bid'), nullable=False)
    dateCreated = Column(String)
    isid = Column(Integer, ForeignKey('iStatus.isid'))
    loid = Column(Integer, ForeignKey('location.loid'))
    checkout = Column(Integer)
    renewal = Column(Integer)
    lastCheckout = Column(String)

    def __repr__(self):
        return "<Item(iid='%s', dateCreated='%s', status='%s', " \
            "location='%s', checkout='%s', renewal='%s', " \
            "lastCheckout='%s')>" % (
                self.iid, self.dateCreated, self.status,
                self.location, self.checkout, self.renewal,
                self.lastCheckout)


class Istatus(Base):
    __tablename__ = 'iStatus'
    isid = Column(Integer, primary_key=True)
    code = Column(String)

    def __repr__(self):
        return "<Istatus(isid='%s', code='%s')>" % (
            self.isid, self.code)


class Location(Base):
    __tablename__ = 'location'
    loid = Column(Integer, primary_key=True)
    code = Column(String)

    def __repr__(self):
        return "<Location(loid='%s', code='%s')>" % (
            self.loid, self.code)


class DataAccessLayer:
    def __init__(self):
        self.conn_string = conn_string
        self.engine = None
        self.session = None

    def connect(self):
        self.engine = create_engine(self.conn_string)
        self.Session = sessionmaker(bind=self.engine)


dal = DataAccessLayer()


@contextmanager
def session_scope():
    dal.connect()
    session = dal.Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def insert_or_ignore(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if not instance:
        instance = model(**kwargs)
        session.add(instance)
    return instance


if __name__ == '__main__':
    # create datastore schema before inserting data
    dal.connect()
    Base.metadata.create_all(dal.engine)


