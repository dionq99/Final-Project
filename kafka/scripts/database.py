from sqlmodel import create_engine, Session

DATABASE_URL = 'postgresql://neondb_owner:npg_MI52VeWAauvC@ep-icy-sunset-a1suvjy5-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'

# Connecting to database
engine = create_engine(DATABASE_URL, echo = True)

# Function to create Database session in router
def get_session():
    return Session(engine)