"""Manages creation of a query plan from a parsed SQL query.

The :class:`SQLQueryPlanner` class is responsible for creating a compute engine
query plan from the AST of the parsed SQL query.

Example:

    >>> from datapyground.sql.parser import Parser
    >>> from datapyground.sql.planner import SQLQueryPlanner
    >>> sql = "SELECT Product, Quantity, Price, Quantity*Price AS Total FROM sales WHERE Product='Videogame' OR Product='Laptop'"
    >>> query = Parser(sql).parse()
    >>> planner = SQLQueryPlanner(query, catalog={"sales": "sales.csv"})
    >>> str(planner.plan())
    "ProjectNode(select=['Product', 'Quantity', 'Price'], project={'Total': pyarrow.compute.multiply(ColumnRef(Quantity),ColumnRef(Price))}, child=FilterNode(filter=pyarrow.compute.or_(pyarrow.compute.equal(ColumnRef(Product),Literal(<pyarrow.StringScalar: 'Videogame'>)),pyarrow.compute.equal(ColumnRef(Product),Literal(<pyarrow.StringScalar: 'Laptop'>))), child=CSVDataSource(sales.csv, block_size=None)))"
"""

import os

import pyarrow.compute as pc

from ..compute import (
    CSVDataSource,
    FilterNode,
    FunctionCallExpression,
    PaginateNode,
    ParquetDataSource,
    ProjectNode,
    SortNode,
    col,
    lit,
)
from ..compute.base import ColumnRef, Expression, Literal, QueryPlanNode


class SQLQueryPlanner:
    """Create a compute engine query plan from a parsed SQL query."""

    FUNCTIONS_MAP = {
        "+": pc.add,
        "-": pc.subtract,
        "*": pc.multiply,
        "/": pc.divide,
        ">": pc.greater,
        "<": pc.less,
        ">=": pc.greater_equal,
        "<=": pc.less_equal,
        "=": pc.equal,
        "AND": pc.and_,
        "OR": pc.or_,
        "NOT": pc.invert,
        "ROUND": pc.round,
        "SUM": pc.sum,
        "COUNT": pc.count,
    }

    def __init__(self, query: dict, catalog: dict[str, str] | None = None) -> None:
        """
        :param query: The parsed SQL query AST as returned by :class:`datapyground.sql.Parser`.
        :param catalog: An optional dictionary mapping table names to file paths.
                        if not provided, it will guess based on files in the current directory.
        """
        self.query = query
        self.catalog = catalog or {}

    def plan(self) -> QueryPlanNode:
        """Generate a query plan from the parsed SQL query."""
        if self.query["type"] == "select":
            return self._plan_select(self.query)
        else:
            raise ValueError(f'Unsupported query type: {self.query["type"]}')

    def _plan_select(self, query: dict) -> QueryPlanNode:
        """Processes a SELECT statement AST by parsing its components.

        :param query: The parsed SELECT statement AST.

        The SELECT statement generates a plan with the following structure::

            - PaginateNode
                - SortNode
                    - ProjectNode
                        - FilterNode
                            - *DataSource

        The structure is based on the fact that:

        - The first thing we want to do is to filter the rows based
          on the WHERE clause as that reduces the amount of data
          we have to deal with.
        - Then we proceed to project the columns we want to keep and
          compute any required projections. This has to happen
          before we run the ORDER BY clause, as we might have
          to sort by a column that was computed in the projection.
        - Then we sort the rows based on the ORDER BY clause.
          This has to happen before the pagination, as we need to
          know the order of the rows to paginate them correctly.
        - Finally, we paginate the rows based on the LIMIT and OFFSET
        """
        datasource = query["from"]

        return self._parse_pagination(
            query.get("offset"),
            query.get("limit"),
            child=self._parse_order_by(
                query.get("order_by"),
                child=self._parse_projections(
                    query["projections"],
                    child=self._parse_where(
                        query.get("where"), child=self._parse_from(datasource)
                    ),
                ),
            ),
        )

    def _parse_projections(
        self, projections: dict, child: QueryPlanNode
    ) -> QueryPlanNode:
        """Parse the projection part of the SELECT statement.

        Creates a :class:`datapyground.compute.ProjectNode`
        with the selected columns and expressions
        based on the provided projections.

        :param projections: The list of projections to apply from the SELECT AST
        :param child: The node to which to apply the projections to.
        """

        def _parse_projection(node: dict) -> tuple[str | None, Expression | ColumnRef]:
            if node["type"] != "projection":
                raise ValueError(f"Unsupported projection type: {node['type']}")

            projection = node["value"]
            alias = node["alias"]
            expr = self._parse_expression(projection)
            if alias is None and not isinstance(expr, ColumnRef):
                raise ValueError(
                    "Projection must have an alias, when it's not a column reference"
                )
            return alias, expr

        parsed_projections = [_parse_projection(p) for p in projections]
        select = [
            p.name
            for alias, p in parsed_projections
            if isinstance(p, ColumnRef) and alias is None
        ]
        project = {alias: p for alias, p in parsed_projections if alias is not None}
        return ProjectNode(select=select, project=project, child=child)

    def _parse_from(self, from_clause: list[str]) -> QueryPlanNode:
        """Parse the FROM clause of the SELECT statement.

        Creates Datasource nodes (IE: :class:`datapyground.compute.CSVDataSource`)
        based on the table names requested in the FROM clause.

        If the table name is not found in the catalog, it will try to guess
        the file path based on the current directory.

        :param from_clause: The list of table names in the FROM clause.
        """
        if len(from_clause) != 1:
            raise ValueError("Only single table queries are supported")

        tablename = from_clause[0]
        if tablename in self.catalog:
            filename = self.catalog[tablename]
        elif os.path.exists(tablename + ".csv"):
            filename = tablename + ".csv"
        elif os.path.exists(tablename + ".parquet"):
            filename = tablename + ".parquet"
        else:
            filename = ""

        if filename.endswith(".csv"):
            return CSVDataSource(filename)
        elif filename.endswith(".parquet"):
            return ParquetDataSource(filename)
        else:
            raise NotImplementedError(f"File format not supported: {filename}")

    def _parse_where(
        self, where_clause: dict | None, child: QueryPlanNode
    ) -> QueryPlanNode:
        """Parse the WHERE clause of the SELECT statement.

        If the where clause is not provided, it will return the child node as is.
        Otherwise it will process the expression in the WHERE clause
        and create a :class:`datapyground.compute.FilterNode` node.

        The expression must be a boolean expression, returning a
        mask to filter the rows, otherwise behavior is unpredictable.

        :param where_clause: The WHERE clause AST.
        """
        if where_clause is None:
            return child

        return FilterNode(self._parse_expression(where_clause), child=child)

    def _parse_pagination(
        self, offset: int | None, limit: int | None, child: QueryPlanNode
    ) -> QueryPlanNode:
        """Parse the LIMIT and OFFSET clauses of the SELECT statement.

        If the limit or offset are not provided, it will return the child node as is.
        Otherwise it will process the limit and offset values and return a new
        :class:`datapyground.compute.base.QueryPlanNode` with the pagination applied.

        :param offset: The OFFSET value.
        :param limit: The LIMIT value.
        :param child: The child node to apply the pagination to.
        """
        if offset is None and limit is None:
            return child

        return PaginateNode(offset=offset, length=limit, child=child)

    def _parse_order_by(
        self, order_by: list[dict] | None, child: QueryPlanNode
    ) -> QueryPlanNode:
        """Parse the ORDER BY clause of the SELECT statement.

        Creates a :class:`datapyground.compute.SortNode` node with the columns
        to sort by and the direction of the sort.

        :param order_by: The list of columns to sort by as provided by the AST.
        :param child: The child node to apply the sorting to.
        """
        if order_by is None:
            return child

        keys = []
        descending = []
        for ordering in order_by:
            keys.append(ordering["column"])
            if ordering["order"].lower() == "desc":
                descending.append(True)
            else:
                descending.append(False)

        return SortNode(keys=keys, descending=descending, child=child)

    def _parse_identifier(self, node: dict) -> ColumnRef:
        """Parse an identifier node from the AST."""
        if node["type"] != "identifier":
            raise ValueError(f"Unsupported identifier type: {node['type']}")
        return col(node["value"])

    def _parse_literal(self, node: dict) -> Literal:
        """Parse a literal node from the AST."""
        if node["type"] != "literal":
            raise ValueError(f"Unsupported literal type: {node['type']}")
        return lit(node["value"])

    def _parse_expression(self, node: dict) -> Expression:
        """Parse an expression node from the AST.

        Traverses the AST and creates a :class:`datapyground.compute.FunctionCallExpression`,
        based on the type of the expression node.

        When the node type is one of ``conjunction``, ``binary_op`` or ``comparison``,
        it will recursively parse the left and right children of the node and create
        a function call expression with the provided operator.

        When the node type is ``unary_op``, it will parse the child of the node
        and create a function call expression with the provided operator.

        When the node type is ``identifier``, it will parse the identifier node.

        When the node type is ``literal``, it will parse the literal node.

        :param node: The expression node from the AST.
        """
        if node["type"] in ("conjunction", "binary_op", "comparison"):
            left = self._parse_expression(node["left"])
            right = self._parse_expression(node["right"])
            return FunctionCallExpression(self.FUNCTIONS_MAP[node["op"]], left, right)
        elif node["type"] == "unary_op":
            return FunctionCallExpression(
                self.FUNCTIONS_MAP[node["op"]], self._parse_expression(node["child"])
            )
        elif node["type"] == "function_call":
            args = [self._parse_expression(arg) for arg in node["args"]]
            return FunctionCallExpression(self.FUNCTIONS_MAP[node["name"]], *args)
        elif node["type"] == "identifier":
            return self._parse_identifier(node)
        elif node["type"] == "literal":
            return self._parse_literal(node)
        else:
            raise ValueError(f"Unsupported expression type: {node['type']}")
