import json
import uuid
from datetime import datetime
from sqlalchemy import create_engine, Column, String, ForeignKey, DateTime
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
engine = create_engine('DATABASE URL') # Escape @ with %40 
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

with open('Json file URL') as f: # Adjust location of the json
    data = json.load(f)


# Find the latest insert
def find_last_inserted_location():
    last_village = session.query(Village).order_by(Village.createdAt.desc()).first()
    if not last_village:
        return None, None, None, None, None
    
    cell = last_village.cell
    sector = cell.sector
    district = sector.district
    province = district.province

    return last_village, cell, sector, district, province


# Continue inserting from latest insert onwards
def insert_data_resuming(data):
    
    last_village, last_cell, last_sector, last_district, last_province = find_last_inserted_location()
    
    for province_name, districts in data.items():
        if last_province and province_name != last_province.province:
            continue
        
        # Check if the province exists or insert it
        province = session.query(Province).filter_by(province=province_name).first()
        if not province:
            province = Province(province=province_name)
            session.add(province)
            session.commit()
            print(f"Inserted Province: {province_name}")
        else:
            print(f"Province '{province_name}' already exists with ID: {province.id}")

        for district_name, sectors in districts.items():
            if last_district and district_name != last_district.district:
                continue

            # Check if the district exists or insert it
            district = session.query(District).filter_by(district=district_name, province_id=province.id).first()
            if not district:
                district = District(district=district_name, province_id=province.id)
                session.add(district)
                session.commit()
                print(f"  Inserted District: {district_name}")
            else:
                print(f"  District '{district_name}' already exists with ID: {district.id}")

            for sector_name, cells in sectors.items():
                if last_sector and sector_name != last_sector.sector:
                    continue

                # Check if the sector exists or insert it
                sector = session.query(Sector).filter_by(sector=sector_name, district_id=district.id).first()
                if not sector:
                    sector = Sector(sector=sector_name, district_id=district.id)
                    session.add(sector)
                    print(f"    Inserted Sector: {sector_name}")
                else:
                    print(f"    Sector '{sector_name}' already exists with ID: {sector.id}")

                # Insert Cells and Villages related to this Sector
                for cell_name, villages in cells.items():
                    cell = session.query(Cell).filter_by(cell=cell_name, sector_id=sector.id).first()
                    if not cell:
                        cell = Cell(cell=cell_name, sector_id=sector.id)
                        session.add(cell)

                    for village_name in villages:
                        village = session.query(Village).filter_by(village=village_name, cell_id=cell.id).first()
                        if not village:
                            village = Village(village=village_name, cell_id=cell.id)
                            session.add(village)

                # Commit everything related to the current Sector
                session.commit()
                print(f"      Committed all data for Sector: {sector_name}")

                # Clear last inserted markers 
                last_cell = None
                last_village = None
                last_sector = None
            last_district = None
        last_province = None



insert_data_resuming(data)
session.close()
print("Yaaay!!!! Script execution completed. PickUp DB is ready to use locations!")
