"""Manages creation of a query plan from a parsed SQL query.

The :class:`SQLQueryPlanner` class is responsible for creating a compute engine
query plan from the AST of the parsed SQL query.

Example:

    >>> import pyarrow as pa
    >>> sales_table = pa.table({"Product": ["Videogame", "Laptop", "Videogame"], "Quantity": [2, 1, 3], "Price": [50, 1000, 60]})
    >>>
    >>> from datapyground.sql.parser import Parser
    >>> from datapyground.sql.planner import SQLQueryPlanner
    >>> sql = "SELECT Product, Quantity, Price, Quantity*Price AS Total FROM sales WHERE Product='Videogame' OR Product='Laptop'"
    >>> query = Parser(sql).parse()
    >>> planner = SQLQueryPlanner(query, catalog={"sales": sales_table})
    >>> str(planner.plan())
    "ProjectNode(select=[], project={'Product': ColumnRef(sales.Product), 'Quantity': ColumnRef(sales.Quantity), 'Price': ColumnRef(sales.Price), 'Total': pyarrow.compute.multiply(ColumnRef(sales.Quantity),ColumnRef(sales.Price))}, child=FilterNode(filter=pyarrow.compute.or_(pyarrow.compute.equal(ColumnRef(sales.Product),Literal(<pyarrow.StringScalar: 'Videogame'>)),pyarrow.compute.equal(ColumnRef(sales.Product),Literal(<pyarrow.StringScalar: 'Laptop'>))), child=ProjectNode(select=[], project={'sales.Product': ColumnRef(Product), 'sales.Quantity': ColumnRef(Quantity), 'sales.Price': ColumnRef(Price)}, child=PyArrowTableDataSource(columns=['Product', 'Quantity', 'Price'], rows=3))))"
"""

import os
from typing import Type

import pyarrow as pa
import pyarrow.compute as pc

from ..compute import (
    AggregateNode,
    CSVDataSource,
    FilterNode,
    FunctionCallExpression,
    InnnerJoinNode,
    PaginateNode,
    ParquetDataSource,
    ProjectNode,
    PyArrowTableDataSource,
    SortNode,
    col,
    lit,
)
from ..compute import aggregate as agg
from ..compute.base import ColumnRef, Expression, Literal, QueryPlanNode
from ..compute.datasources import DataSourceNode


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
    }
    AGGREGATIONS_MAP: dict[str, Type[agg.Aggregation]] = {
        "SUM": agg.SumAggregation,
        "COUNT": agg.CountAggregation,
        "AVG": agg.MeanAggregation,
        "MIN": agg.MinAggregation,
        "MAX": agg.MaxAggregation,
    }
    JOIN_TYPES = {
        "inner": InnnerJoinNode,
    }

    def __init__(
        self, query: dict, catalog: dict[str, str | pa.Table] | None = None
    ) -> None:
        """
        :param query: The parsed SQL query AST as returned by :class:`datapyground.sql.Parser`.
        :param catalog: An optional dictionary mapping table names to file paths.
                        if not provided, it will guess based on files in the current directory.
        """
        self.query = query
        self.catalog = catalog or {}
        self._open_tables: dict[str, pa.Schema] = {}

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
                        - AggregateNode
                            - FilterNode
                                - *DataSource

        The structure is based on the fact that:

        - The first thing we want to do is to filter the rows based
          on the WHERE clause as that reduces the amount of data
          we have to deal with.
        - After we have the data we can process aggregations from GROUP BY, this
          has to happen before the projections, as the projections
          might want to compute derived columns from the aggregations.
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
                    child=self._parse_group_by(
                        query["projections"],
                        query["group_by"],
                        child=self._parse_where(
                            query.get("where"), self._parse_from(datasource)
                        ),
                    ),
                ),
            ),
        )

    def _parse_group_by(
        self,
        projections: dict | None,
        group_by: list[dict] | None,
        child: QueryPlanNode,
    ) -> QueryPlanNode:
        """Parse the aggregations part of the SELECT statement.

        Creates a :class:`datapyground.compute.AggregateNode`
        with the columns that need to compute aggregations.

        :param projections: The list of projections the aggregations should be pulled from.
        :param group_by: The list of identifiers that constitute the keys for the grouping process.
        :param child: The node to which to apply the aggregations to.

        .. note::

            This method will rewrite the projections to remove the aggregations,
            so that subsequent nodes can process them as plain columns.
        """
        if not group_by:
            return child

        if not projections:
            raise ValueError("GROUP BY requires at least one aggregation")

        grouping_keys = [self._parse_identifier(node).name for node in group_by]
        aggregations = {}
        for idx, node in enumerate(projections):
            alias: str = node["alias"]
            expr = self._parse_aggregation(node["value"])
            if expr is not None:
                if not alias:
                    raise ValueError("Aggregations must have an alias")
                aggregations[alias] = expr
                # Rewrite the aggregations as plain projections that the ProjectNode can forward as they are.
                projections[idx] = {
                    "type": "projection",
                    "value": {"type": "identifier", "value": alias},
                    "alias": None,
                }

        return AggregateNode(keys=grouping_keys, aggregations=aggregations, child=child)

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
            if isinstance(expr, ColumnRef):
                if alias is None and expr.name != projection["value"]:
                    # If the ColumnRef name we get is not the same as the projected column name,
                    # it means that the column was namespaced by _parse_identifier but the user
                    # asked for it without the namespace, so we need to keep the original name.
                    alias = projection["value"]
            elif isinstance(expr, agg.Aggregation):
                raise ValueError("Aggregations must be processed by the AggregateNode")
            else:
                if alias is None:
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

    def _parse_from(self, from_clause: list[dict]) -> QueryPlanNode:
        """Parse the FROM clause of the SELECT statement.

        Creates Datasource nodes (IE: :class:`datapyground.compute.CSVDataSource`)
        or Join nodes (IE: :class:`datapyground.compute.InnerJoinNode`)
        based on the FROM clause.

        If table names are not found in the catalog, it will try to guess
        the file path based on the current directory.

        :param from_clause: The list of table names in the FROM clause.
        """
        if len(from_clause) != 1:
            raise ValueError("Only single table queries are supported")

        from_entry = from_clause[0]
        if from_entry["type"] == "identifier":
            # Direct table reference
            return self._open_table(from_entry)
        elif from_entry["type"] == "join":
            if from_entry["join_type"] != "inner":
                raise ValueError("Only inner joins are supported")
            left_table = self._open_table(from_entry["left_table"])
            right_table = self._open_table(from_entry["right_table"])
            condition = from_entry["join_condition"]
            if condition["type"] != "comparison" or condition["op"] != "=":
                raise ValueError("Only comparison joins are supported")
            left_key = self._parse_identifier(condition["left"]).name
            right_key = self._parse_identifier(condition["right"]).name

            join_type = from_entry["join_type"]
            if join_type not in self.JOIN_TYPES:
                raise ValueError(
                    f"Unsupported join type: {join_type}, only {self.JOIN_TYPES.keys()} are supported"
                )
            return self.JOIN_TYPES[join_type](
                left_key, right_key, left_table, right_table
            )
        else:
            raise ValueError(f"Unsupported FROM entry type: {from_entry['type']}")

    def _open_table(self, identifier: dict) -> QueryPlanNode:
        """Open a table from the catalog and return a data source node.

        Also loads the schema of the table, to make the planner aware of the columns.

        :param identifier: The identifier node of the table from the AST.
        """
        if identifier["type"] != "identifier":
            raise ValueError(
                f"Unsupported node type: {identifier['type']}, expecting an identifier"
            )

        tablename = identifier["value"]
        if tablename in self._open_tables:
            raise ValueError(f"Table {tablename} was already opened")

        data_source: DataSourceNode | None = None
        if isinstance(self.catalog.get(tablename), (pa.Table, pa.RecordBatch)):
            data_source = PyArrowTableDataSource(self.catalog[tablename])
        else:
            if tablename in self.catalog:
                filename = self.catalog[tablename]
            elif os.path.exists(tablename + ".csv"):
                filename = tablename + ".csv"
            elif os.path.exists(tablename + ".parquet"):
                filename = tablename + ".parquet"
            else:
                filename = ""

            if filename.endswith(".csv"):
                data_source = CSVDataSource(filename)
            elif filename.endswith(".parquet"):
                data_source = ParquetDataSource(filename)
            else:
                raise NotImplementedError(f"File format not supported: {filename}")
        self._open_tables[tablename] = data_source.poll_schema()

        # Wrap the data source in a ProjectNode to make all
        # column names explicit.
        return ProjectNode(
            select=[],  # Keep no original columns, only the namespaced ones.
            project={
                f"{tablename}.{c}": col(c) for c in self._open_tables[tablename].names
            },
            child=data_source,
        )

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
            keys.append(self._parse_identifier(ordering["column"]).name)
            if ordering["order"].lower() == "desc":
                descending.append(True)
            else:
                descending.append(False)

        return SortNode(keys=keys, descending=descending, child=child)

    def _parse_identifier(self, node: dict) -> ColumnRef:
        """Parse an identifier node from the AST."""
        if node["type"] != "identifier":
            raise ValueError(f"Unsupported identifier type: {node['type']}")

        value = node["value"]
        if "." not in value:
            # The identifier is not properly namespaced,
            # we need to find the table it belongs to.
            tablename = None
            for table, schema in self._open_tables.items():
                if value in schema.names:
                    if tablename is not None:
                        raise ValueError(f"Ambiguous column name: {value}")
                    tablename = table
            if tablename is not None:
                # The column belongs to a table, we need to namespace it.
                # Otherwise, we take for granted that it's a computed or renamed column.
                # so it's up to the user to ensure it's unique.
                value = f"{tablename}.{value}"
        return col(value)

    def _parse_literal(self, node: dict) -> Literal:
        """Parse a literal node from the AST."""
        if node["type"] != "literal":
            raise ValueError(f"Unsupported literal type: {node['type']}")
        return lit(node["value"])

    def _parse_aggregation(self, node: dict) -> agg.Aggregation | None:
        """Parse an aggregation function from the AST.

        If a supported aggregation function is found,
        it will create a :class:`datapyground.compute.Aggregation`.
        Returns ``None`` if it's not an aggregation or it's not supported.

        :param node: The aggregation node from the AST.
        """
        if node["type"] == "function_call":
            function_name = node["name"]
            if function_name in self.AGGREGATIONS_MAP:
                columns = [self._parse_identifier(arg).name for arg in node["args"]]
                return self.AGGREGATIONS_MAP[function_name](*columns)
        return None

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
            if node["name"] in self.FUNCTIONS_MAP:
                args = [self._parse_expression(arg) for arg in node["args"]]
                return FunctionCallExpression(self.FUNCTIONS_MAP[node["name"]], *args)
            else:
                raise ValueError(f"Unsupported function: {node['name']}")
        elif node["type"] == "identifier":
            return self._parse_identifier(node)
        elif node["type"] == "literal":
            return self._parse_literal(node)
        else:
            raise ValueError(f"Unsupported expression type: {node['type']}")
