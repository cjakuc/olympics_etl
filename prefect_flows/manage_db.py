from sqlalchemy import create_engine, Column, String, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select
from sqlalchemy.orm import relationship
from dotenv import load_dotenv
from os import environ

# Run this file to create empty database tables

load_dotenv()
POSTGRES_USER = environ["POSTGRES_USER"]
POSTGRES_DB = environ["POSTGRES_DB"]
POSTGRES_PASSWORD = environ["POSTGRES_PASSWORD"]
POSTGRES_PORT = environ["POSTGRES_PORT"]
POSTGRES_SERVER = environ["POSTGRES_SERVER"]
engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}')

Base = declarative_base()


class Regions(Base):
    """
    Regions table SQLAlchemy class

    Parameters
    ----------
    Base :
        SQLAlchemy declarative_base instance
    """
    __tablename__ = "regions"
    __table_args__ = {'extend_existing': True}
    NOC = Column(String, primary_key=True)
    REGION = Column(String)
    NOTES = Column(String)

class Athletes(Base):
    """
    Athletes table SQLAlchemy class

    Parameters
    ----------
    Base :
        SQLAlchemy declarative_base instance
    """
    __tablename__ = "athletes"
    __table_args__ = {'extend_existing': True}
    ATHLETEID = Column(String, primary_key=True)
    NAME = Column(String)
    SEX = Column(String)
    AGE = Column(Integer)
    NOC = Column(String, ForeignKey('regions.NOC'))
    TEAM = Column(String)
    regions = relationship("regions", back_populates = "athletes")

class EventResults(Base):
    """
    EventResults table SQLAlchemy class

    Parameters
    ----------
    Base :
        SQLAlchemy declarative_base instance
    """
    __tablename__ = "event_results"
    __table_args__ = {'extend_existing': True}
    EVENTRESULTID = Column(String, primary_key=True)
    YEAR = Column(Integer)
    SEASON = Column(String)
    CITY = Column(String)
    SPORT = Column(String)
    EVENT = Column(String)
    MEDAL = Column(String)
    ATHLETEID = Column(String, ForeignKey('athletes.ATHLETEID'))
    athletes = relationship("athletes", back_populates = "event_results")


if __name__ == '__main__':
    Regions.__table__.create(bind=engine, checkfirst=True)
    Athletes.__table__.create(bind=engine, checkfirst=True)
    EventResults.__table__.create(bind=engine, checkfirst=True)
