# -*- coding:utf-8 -*-
import sys


def collect(file_name):
    info = {}
    cases = []
    for line in open(file_name):
        parts = [_.strip() for _ in line.split(" ") if _.strip() != ""]
        if len(parts) < 4:
            continue
        try:
            case_name = parts[0]
            time = float(parts[1])
            cpu_time = float(parts[3])
            cases.append(case_name)
            info[case_name] = (time, cpu_time)
        except:
            print("Skip line: \"" + line.strip() + "\"")
    return info, cases


def compare(base_file, cur_file):
    html = """
        <html><head>
            <meta charset="utf-8">
            <style type="text/css">
                th { border: 1px solid black; text-align: center; 
                  padding: 3px; background-color: green; color: white;}
                td { border: 1px solid black; text-align: center; 
                  padding: 3px; }
                .posrate { color: green; }
                .negrate { color: red; }
            </style>
        </head><body>
        <table border="1">
        <tr>
            <th>Case</th>
            <th>总耗时</th>
            <th>基线总耗时</th>
            <th>总耗时变化</th>
            <th>CPU耗时</th>
            <th>基线CPU耗时</th>
            <th>CPU耗时变化</th>
        </tr>
        ${CASES}
        </table>
        </body></html>
    """
    
    rows = []
    base_dict, base_cases = collect(base_file)
    cur_dict, _ = collect(cur_file)
    for case_name in base_cases:
        if not case_name in cur_dict:
            continue
        (total_time, cpu_time) = cur_dict[case_name]
        (base_total_time, base_cpu_time) = base_dict[case_name]
        total_time_rate = float(total_time - base_total_time) / base_total_time
        cpu_time_rate = float(cpu_time - base_cpu_time) / base_cpu_time

        def create_rate_str(rate):
            rate = rate * 100
            if rate > 5:
                return "<div class=\"negrate\">+%.1f%%</div>" % rate
            elif rate < -5:
                return "<div class=\"posrate\">%.1f%%</div>" % rate
            else:
                return "%.1f" % rate

        row = """<tr>
                <td>%s</td>
                <td>%d</td>
                <td>%d</td>
                <td>%s</td>
                <td>%d</td>
                <td>%d</td>
                <td>%s</td>
            </tr>""" % (case_name,
                total_time, base_total_time, create_rate_str(total_time_rate),
                cpu_time, base_cpu_time, create_rate_str(cpu_time_rate))
        rows.append(row)

    html = html.replace("${CASES}", "\n".join(rows))
    with open("./benchmark_compare.html", "w") as f:
        f.write(html)


if __name__ == "__main__":
    compare(sys.argv[1], sys.argv[2])

