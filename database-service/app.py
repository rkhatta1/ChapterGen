from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, JSON, ForeignKey, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from pydantic import BaseModel
import traceback

DATABASE_URL = "postgresql://admin:password@postgres-service:5432/chapterdb"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# SQLAlchemy Models
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    name = Column(String)
    jobs = relationship("Job", back_populates="owner")

class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, index=True)
    youtube_url = Column(String)
    status = Column(String)
    chapters = Column(JSON)
    owner_id = Column(Integer, ForeignKey("users.id"))
    owner = relationship("User", back_populates="jobs")

# Pydantic Models (for API validation)
class UserCreate(BaseModel):
    email: str
    name: str

class UserResponse(UserCreate):
    id: int

    class Config:
        from_attributes = True

class JobCreate(BaseModel):
    youtube_url: str
    owner_email: str

app = FastAPI()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

@app.post("/users/", response_model=UserResponse)
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    db_user = User(email=user.email, name=user.name)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

@app.get("/users/by-email/{email}", response_model=UserResponse)
def get_user_by_email(email: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == email).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@app.post("/jobs/")
def create_job(job: JobCreate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == job.owner_email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    db_job = Job(youtube_url=job.youtube_url, owner_id=user.id, status="pending", chapters={})
    db.add(db_job)
    db.commit()
    db.refresh(db_job)
    return db_job

@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    try:
        # Try to execute a simple query to check database connectivity
        db.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        print(f"Database connection failed: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")
