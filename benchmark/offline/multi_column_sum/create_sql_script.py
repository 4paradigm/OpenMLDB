import sys
import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cols", type=int, required=True, help="Final output columns")
    parser.add_argument("--offset", type=int, required=True, help="Final output columns")
    parser.add_argument("output", type=str, required=True, help="Output script file")
    return parser.parse_args(sys.argv[1:])


def main(args):
    content = """SELECT {EXPRS} FROM t1 
        WINDOW w AS
        (PARTITION BY t1.id ORDER BY t1.`time`
        ROWS BETWEEN {OFFSET} PRECEDING AND CURRENT ROW)"""
    with open(args.output, "w") as output_file:
        exprs = []
        for k in range(args.cols):
            exprs.append("sum(c" + k + ") OVER w")
        content = content.replace("{EXPRS}", ",\n".join(exprs))
        content = content.replace("{OFFSET}", str(args.offset))
        output_file.write(content)


if __name__ == "__main__":
    main(parse_args())

