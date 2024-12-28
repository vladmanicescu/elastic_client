import argparse
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( "-d", "--daily-one-month",
                        help="Run the script with daily queries for one month", action="store_true")
    parser.add_argument( "-m", "--one-query-prev-month",
                        help="Run the script using one query for the previous month", action="store_true")
    parser.add_argument( "-o", "--outbound",
                        help="Generate outbound report", action="store_true")
    parser.add_argument( "-i", "--inbound",
                        help="Generate inbound report", action="store_true")
    options = parser.parse_args()
    return options