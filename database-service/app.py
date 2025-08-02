from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, JSON, ForeignKey, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from pydantic import BaseModel
import traceback
from typing import List
from fastapi.middleware.cors import CORSMiddleware


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
    video_id = Column(String, index=True)
    title = Column(String)
    description = Column(String)
    thumbnail_url = Column(String)
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
    video_id: str
    title: str
    description: str
    thumbnail_url: str
    owner_email: str

class JobResponse(BaseModel):
    id: int
    video_id: str
    title: str
    description: str
    thumbnail_url: str
    status: str
    chapters: dict

    class Config:
        from_attributes = True

class JobStatusUpdate(BaseModel):
    status: str

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

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

@app.post("/jobs/", response_model=JobResponse)
def create_job(job: JobCreate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == job.owner_email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    db_job = Job(
        video_id=job.video_id,
        title=job.title,
        description=job.description,
        thumbnail_url=job.thumbnail_url,
        owner_id=user.id,
        status="pending",
        chapters={}
    )
    db.add(db_job)
    db.commit()
    db.refresh(db_job)
    return db_job

@app.get("/jobs/by-user/{email}", response_model=List[JobResponse])
def get_jobs_by_user_email(email: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == email).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user.jobs

@app.get("/jobs/by-video-id/{video_id}", response_model=JobResponse)
def get_job_by_video_id(video_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.video_id == video_id).first()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.put("/jobs/{video_id}/chapters")
def update_job_chapters(video_id: str, chapters: dict, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.video_id == video_id).first()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    job.chapters = chapters
    job.status = "completed"
    db.commit()
    db.refresh(job)
    return job

# --- THIS IS THE NEW ENDPOINT ---
@app.put("/jobs/{video_id}/status", response_model=JobResponse)
def update_job_status(video_id: str, status_update: JobStatusUpdate, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.video_id == video_id).first()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job.status = status_update.status
    db.commit()
    db.refresh(job)
    return job

@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    try:
        # Ensure tables are created. This is idempotent.
        Base.metadata.create_all(bind=engine)
        # Try to execute a simple query to check database connectivity
        db.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        print(f"Database connection failed: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")