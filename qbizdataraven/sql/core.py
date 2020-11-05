from collections.abc import Iterable

import sqlalchemy.sql.expression as sql
from sqlalchemy import text

from .helpers import format_select_columns, format_from_clause, apply_where_clause


def build_select_query(from_clause, *columns, where_clause=None, select_distinct=False):
    """
    :param from_clause: sqlalchemy TextClause, Select, Alias or Join object
    :param columns: column names to be selected; expected str, TextClause or ColumnClause
    :param where_clause: predicate to apply to where clause in query; expected str, TextClause, BinaryExpression
    :param select_distinct: change SELECT to SELECT DISTINCT
    :return: sqlalchemy.sql.selectable.Select object
    """
    columns_ = format_select_columns(*columns)
    query = sql.select(columns_)

    if select_distinct is True:
        query = query.distinct()

    from_clause_ = format_from_clause(from_clause)
    query = query.select_from(from_clause_)

    if isinstance(where_clause, Iterable):
        for predicate in where_clause:
            query = apply_where_clause(query, predicate)
    elif where_clause is not None:
        query = apply_where_clause(query, where_clause)

    return query


def build_aggregate_query(from_clause, *groupby_columns, where_clause=None, **aggregate_columns):
    """
    :param from_clause: sqlalchemy TextClause, Select, Alias or Join object
    :param groupby_columns: column names to be used in group by clause; expected str, TextClause or ColumnClause
    :param where_clause: predicate to apply to where clause in query; expected str, TextClause, BinaryExpression
    :param aggregate_columns: key value pairs of the form column_name=func where column_name is the name of table
    column and func is an aggregate function from sqlalchemy.sql.func
    :return: sqlalchemy.sql.selectable.Select object
    """
    from_clause_ = format_from_clause(from_clause)
    aggregates = []
    for col in aggregate_columns:
        col_ = text(col)
        func = aggregate_columns[col]
        agg_column_label = col
        agg_column = func(col_).label(agg_column_label)
        aggregates.append(agg_column)

    groupby_columns_ = format_select_columns(*groupby_columns)

    query_select_clause = list(groupby_columns_)
    query_select_clause.extend(aggregates)

    query = sql.select(query_select_clause).select_from(from_clause_)

    if isinstance(where_clause, Iterable):
        for predicate in where_clause:
            query = apply_where_clause(query, predicate)
    elif where_clause is not None:
        query = apply_where_clause(query, where_clause)

    query = query.group_by(*groupby_columns_)
    return query
