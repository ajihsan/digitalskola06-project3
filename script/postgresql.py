from sqlalchemy import create_engine
from psycopg2 import connect
from psycopg2.extras import execute_values


class PostgreSQL:
  def __init__(self, cfg):
      self.host = cfg['host']
      self.port = cfg['port']
      self.username = cfg['username']
      self.password = cfg['password']
      self.database = cfg['database']

