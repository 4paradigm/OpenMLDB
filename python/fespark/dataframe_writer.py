# dataframe_writer.py
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


class DataframeWriter(object):

    def __init__(self, jdf):
        self.jdf = jdf
    
    def parquet(self, path, mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, "parquet", mode, renameDuplicateColumns, partitionNum)

    def csv(self, path, mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, "csv", mode, renameDuplicateColumns, partitionNum)

    def json(self, path, mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, "json", mode, renameDuplicateColumns, partitionNum)

    def text(self, path, mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, "text", mode, renameDuplicateColumns, partitionNum)

    def orc(self, path, mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, "orc", mode, renameDuplicateColumns, partitionNum)

    def mode(self, mode):
        # TODO: Not use pyspark mode yet
        return self
