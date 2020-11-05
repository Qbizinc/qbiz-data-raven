from sqlalchemy.dialects.postgresql import dialect as PostgresDialect
from sqlalchemy.dialects.mysql import dialect as MySQLDialect
from sqlalchemy import text, column
from sqlalchemy.sql.elements import TextClause, ColumnClause
from sqlalchemy.sql.selectable import Select, Alias, Join


def apply_where_clause(query, where_clause):
    """
    :param query: sqlalchemy.sql.selectable.Select object
    :param where_clause: predicate to apply to where clause in query; expected str, TextClause, BinaryExpression
    :return: query with where clause logic added
    """
    if isinstance(where_clause, str):
        where_clause_ = text(where_clause)
        query_ = query.where(where_clause_)
        return query_
    else:
        try:
            query_ = query.where(where_clause)
            return query_
        except TypeError:
            type_ = type(where_clause)
            error_msg = f"{apply_where_clause.__name__}, caught unexpected type for where_clause: {type_}"
            raise TypeError(error_msg)
        except Exception as ex:
            raise ex


def format_from_clause(from_clause):
    """
    :param from_clause: used as query from clause; expected sqlalchemy TextClause, Select, Alias or Join object
    :return: return formatted from clause
    """
    expected_sqlalchemy_classes = [TextClause, Select, Alias, Join]
    condition = any(map(lambda class_: isinstance(from_clause, class_), expected_sqlalchemy_classes))

    if isinstance(from_clause, str):
        return text(from_clause)
    elif condition is True:
        return from_clause
    else:
        expected_sqlalchemy_classes_ = ','.join(list(map(lambda class_: str(class_.__name__),
                                                         expected_sqlalchemy_classes)))
        type_ = type(from_clause)
        error_msg = f"expected from_clause to be type: {expected_sqlalchemy_classes_} but found {type_}"
        raise TypeError(error_msg)


def format_select_columns(*columns):
    """
    :param columns: column names to be selected; expected str, TextClause or ColumnClause
    :return: tuple of column names
    """
    output = []
    for col in columns:
        if isinstance(col, str):
            output.append(column(col))
        elif isinstance(col, TextClause):
            output.append(column(str(col)))
        elif isinstance(col, ColumnClause):
            output.append(col)
        else:
            type_ = type(col)
            error_msg = f"expected column to be of type str, TextClause or ColumnClause but found {type_}"
            raise TypeError(error_msg)
    return tuple(output)


def compile_to_dialect(query, dialect, use_ansi=True):
    """
    :param query: sqlalchemy.sql.selectable.Select object
    :param dialect: str to be used as key to get correct dialect
    :param use_ansi: compile to ansi compliant sql if True
    :return: formatted query to specified dialect
    """
    dialect_ = dialect.upper()
    if dialect_ == "POSTGRES":
        query_ = query.compile(dialect=PostgresDialect(use_ansi=use_ansi))
    elif dialect_ == "MYSQL":
        query_ = query.compile(dialect=MySQLDialect(use_ansi=use_ansi))
    else:
        error_msg = f"expected one fo the following dialects: POSTGRES, MYSQL ... but found {dialect}"
        raise ValueError(error_msg)
    return query_





