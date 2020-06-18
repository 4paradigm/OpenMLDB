from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)

@magics_class
class SqlMagic(Magics):
  # The notebook magic to run SQL, docs in https://ipython.org/ipython-doc/3/config/custommagics.html

  def __init__(self, shell, sess):
    super(SqlMagic, self).__init__(shell)
    self.sess = sess

  @line_cell_magic
  def sql(self, line, cell=None):
    if cell is None:
      sqlText = line
    else:
      sqlText = cell.replace("\n", " ")
    self.sess.sql(sqlText).show()

def register(sess):
  ip = get_ipython()
  magics = SqlMagic(ip, sess)
  ip.register_magics(magics)
