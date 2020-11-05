

def measure_null(row, *columns, null_values=None):
    """
    :param row:
    :param column_indicies:
    :param null_values:
    :return:
    """
    null_values_ = {"NULL"}
    if null_values is not None:
        null_values_ = null_values_.union(null_values)

    result = {}
    for column in columns:
        value = row[column]
        if value in null_values_:
            result[column] = 1
        else:
            result[column] = 0
    output = {"result": result}
    return output


def measure_duplicates(row, *columns, collection=None):
    """
    :param row:
    :param column_indicies:
    :param collection:
    :return:
    """
    result = {}
    if collection is None:
        collection = {}

    for column in columns:
        value = row[column]
        key = (column, value)
        if key not in collection:
            collection[key] = None
            result[column] = 0
        else:
            result[column] = 1

    output = {"result": result, "collection": collection}
    return output


def measure_set_duplicates(row, *columns, collection=None):
    """
    :param row:
    :param columns:
    :param collection:
    :return:
    """
    if collection is None:
        collection = {}

    values = tuple(map(lambda col: row[col], columns))
    key = tuple(zip(columns, values))

    columns_label = ",".join(columns)
    if key not in collection:
        collection[key] = None
        result = {columns_label: 0}
    else:
        result = {columns_label: 1}
    output = {"result": result, "collection": collection}
    return output
