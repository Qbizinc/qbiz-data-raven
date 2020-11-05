import csv


def get_csv_document(path, delimiter=',', fieldnames=None):
    """
    :param path: path to csv file
    :param delimiter: separator used in csv file
    :return: list of tuples where each tuple is a row in csv file
    """
    with open(path, 'r') as infile:
        csvreader = csv.DictReader(infile, delimiter=delimiter, fieldnames=fieldnames)
        dataset = list(csvreader)
    infile.close()
    return dataset


def apply_reducer(dataset, reducer, *columns, **kwargs):
    """
    :param dataset:
    :param measure:
    :param accum:
    :param args:
    :param is_header_included:
    :param kwargs:
    :return:
    """
    rowcnt = 0
    accum = {}
    for row in dataset:
        rowcnt += 1
        output = reducer(row, *columns, **kwargs)

        result = output["result"]
        collection = output.get("collection")
        if collection is not None:
            kwargs["collection"] = collection

        for column in result:
            accum[column] = accum.get(column, 0) + result[column]

    results = {"rowcnt": rowcnt, "accum": accum}
    return results
