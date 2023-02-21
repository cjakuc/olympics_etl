from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData
from dotenv import load_dotenv
import os

# Creates ERD. Must run `apt-get install graphviz` in container before running

load_dotenv()
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_SERVER = os.environ["POSTGRES_SERVER"]

graph = create_schema_graph(metadata=MetaData(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DB}'),
   show_datatypes=True,
   show_indexes=False,
   rankdir='LR', 
   concentrate=True
)
graph.write_png('dbschema.png') # write out the file
