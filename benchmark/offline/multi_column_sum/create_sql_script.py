# create_sql_script.py
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

import sys
import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cols", type=int, required=True, help="Final output columns")
    parser.add_argument("--offset", type=int, required=True, help="Final output columns")
    parser.add_argument("output", type=str, help="Output script file")
    return parser.parse_args(sys.argv[1:])


def main(args):
    content = """SELECT {EXPRS} FROM t1 
        WINDOW w AS
        (PARTITION BY t1.id ORDER BY t1.`time`
        ROWS BETWEEN {OFFSET} PRECEDING AND CURRENT ROW)"""
    with open(args.output, "w") as output_file:
        exprs = []
        for k in range(args.cols):
            exprs.append("sum(c%d) OVER w" % k)
        content = content.replace("{EXPRS}", ",\n".join(exprs))
        content = content.replace("{OFFSET}", str(args.offset))
        output_file.write(content)


if __name__ == "__main__":
    main(parse_args())

