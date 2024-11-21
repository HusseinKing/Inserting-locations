import json
import uuid
from datetime import datetime
from sqlalchemy import create_engine, Column, String, ForeignKey, DateTime
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import os

# Database setup
Base = declarative_base()
engine = create_engine('BD_url')
Session = sessionmaker(bind=engine)
session = Session()

# Models
class Province(Base):
    __tablename__ = 'Provinces'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    province = Column(String(255), nullable=False, unique=True)
    createdAt = Column(DateTime, nullable=False, default=datetime.utcnow)
    updatedAt = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    districts = relationship("District", back_populates="province")


class District(Base):
    __tablename__ = 'Districts'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    district = Column(String(255), nullable=False, unique=True)
    province_id = Column(String(36), ForeignKey('Provinces.id'), nullable=False)
    createdAt = Column(DateTime, nullable=False, default=datetime.utcnow)
    updatedAt = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    province = relationship("Province", back_populates="districts")
    sectors = relationship("Sector", back_populates="district")


class Sector(Base):
    __tablename__ = 'Sectors'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    sector = Column(String(255), nullable=False)
    district_id = Column(String(36), ForeignKey('Districts.id'), nullable=False)
    createdAt = Column(DateTime, nullable=False, default=datetime.utcnow)
    updatedAt = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    district = relationship("District", back_populates="sectors")
    cells = relationship("Cell", back_populates="sector")


class Cell(Base):
    __tablename__ = 'Cells'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    cell = Column(String(255), nullable=False)
    sector_id = Column(String(36), ForeignKey('Sectors.id'), nullable=False)
    createdAt = Column(DateTime, nullable=False, default=datetime.utcnow)
    updatedAt = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    sector = relationship("Sector", back_populates="cells")
    villages = relationship("Village", back_populates="cell")


class Village(Base):
    __tablename__ = 'Villages'
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    village = Column(String(255), nullable=False)
    cell_id = Column(String(36), ForeignKey('Cells.id'), nullable=False)
    createdAt = Column(DateTime, nullable=False, default=datetime.utcnow)
    updatedAt = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    cell = relationship("Cell", back_populates="villages")


# Progress tracker
TRACKER_FILE = "progress_tracker.json"

def load_progress():
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE, 'r') as f:
            return json.load(f)
    return {"province": None, "district": None, "sector": None, "cell": None, "village": None}

def save_progress(progress):
    with open(TRACKER_FILE, 'w') as f:
        json.dump(progress, f)

with open('Desktop/georwanda.json') as f:
    data = json.load(f)

def insert_data(data):
    # Load progress
    progress = load_progress()

    for province_name, districts in data.items():
        if progress["province"] and province_name < progress["province"]:
            continue
        progress["province"] = province_name
        save_progress(progress)

        province = session.query(Province).filter_by(province=province_name).first()
        if not province:
            province = Province(province=province_name)
            session.add(province)
            session.flush()
            print(f"Inserted Province: {province_name}")
        
        for district_name, sectors in districts.items():
            if progress["district"] and district_name < progress["district"]:
                continue
            progress["district"] = district_name
            save_progress(progress)

            district = session.query(District).filter_by(district=district_name, province_id=province.id).first()
            if not district:
                district = District(district=district_name, province_id=province.id)
                session.add(district)
                session.flush()
                print(f"  Inserted District: {district_name}")

            for sector_name, cells in sectors.items():
                if progress["sector"] and sector_name < progress["sector"]:
                    continue
                progress["sector"] = sector_name
                save_progress(progress)

                sector = session.query(Sector).filter_by(sector=sector_name, district_id=district.id).first()
                if not sector:
                    sector = Sector(sector=sector_name, district_id=district.id)
                    session.add(sector)
                    session.flush()
                    print(f"    Inserted Sector: {sector_name}")

                for cell_name, villages in cells.items():
                    if progress["cell"] and cell_name < progress["cell"]:
                        continue
                    progress["cell"] = cell_name
                    save_progress(progress)

                    cell = session.query(Cell).filter_by(cell=cell_name, sector_id=sector.id).first()
                    if not cell:
                        cell = Cell(cell=cell_name, sector_id=sector.id)
                        session.add(cell)
                        session.flush()
                        print(f"      Inserted Cell: {cell_name}")

                    for village_name in villages:
                        if progress["village"] and village_name < progress["village"]:
                            continue
                        progress["village"] = village_name
                        save_progress(progress)

                        village = session.query(Village).filter_by(village=village_name, cell_id=cell.id).first()
                        if not village:
                            village = Village(village=village_name, cell_id=cell.id)
                            session.add(village)
                            print(f"        Inserted Village: {village_name}")

    # Commit all changes
    session.commit()
    print("Completed insertion with resume support.")

insert_data(data)
session.close()
