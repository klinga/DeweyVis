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
    status = Column(String)
    itemCount = Column(Integer)
    callNo = Column(String)
    country = Column(String)
    pubDate = Column(String)
    lang = Column(String)
    catSource = Column(String)
    catAgency = Column(String)

    items = relationship('Item', cascade='all, delete-orphan')

    def __repr__(self):
        return "<Bib(bid='%s', dateCreated='%s', dateCataloged='%s', " \
            "status='%s', itemCount='%s', callNo='%s', country='%s', " \
            "lang='%s', catSource='%s', catAgency='%s')>" % (
                self.id, self.dateCreated, self.dateCataloged,
                self.status, self.itemCount, self.callNo, self.country,
                self.lang, self.catSource, self.catAgency)


class Item(Base):
    __tablename__ = 'item'
    iid = Column(Integer, primary_key=True, autoincrement=False)
    bid = Column(Integer, ForeignKey('bib.bid'), nullable=False)
    dateCreated = Column(String)
    status = Column(String)
    location = Column(String)
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


class DataAccessLayer:
    def __init__(self):
        self.conn_string = conn_string
        self.engine = None
        self.session = None

    def connect(self):
        self.engine = create_engine(self.conn_string)
        Base.metadata.create_all(self.engine)
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
        raise
    finally:
        session.close()


def insert_or_ignore(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if not instance:
        instance = model(**kwargs)
        session.add(instance)

if __name__ == '__main__':
    dal.connect()

