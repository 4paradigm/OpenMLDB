# sql_magic.py
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
