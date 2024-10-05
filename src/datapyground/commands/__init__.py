"""Shell commands exposing DataPyground functionalities.

This module contains the shell commands that can be used to interact with DataPyground.

FQuery (file query)
===================

``pyground-fquery`` runs SQL queries on files::

    pyground-fquery -t users=users.csv "SELECT id, name FROM users WHERE age >= 18"

It can be tested against provided example data running it with the following command::

    pyground-fquery -t sales=examples/data/sales.csv "SELECT Product, Quantity, Price, Quantity*Price AS Total FROM sales WHERE Product='Videogame' OR Product='Laptop' ORDER BY Total DESC LIMIT 5"

"""
