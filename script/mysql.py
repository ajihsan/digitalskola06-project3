from sqlalchemy import create_engine


class MySQL:
  def __init__(self, cfg):
    self.host = cfg['host']
    self.port = cfg['port']
    self.username = cfg['username']
    self.password = cfg['password']
    self.database = cfg['database']

