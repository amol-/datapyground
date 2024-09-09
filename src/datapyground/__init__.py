"""DataPyground

A data platform built from scratch for learning and teaching purposes.

DataPyground was started as a foundation for the `How Data Platforms Work <https://github.com/amol-/datapyground/tree/main/book>`_
book associated to the `Monthly Python Data Engineering Newsletter <https://alessandromolina.substack.com/>`_
while writing the book to showcase the concepts explained in the its chapters.

The platform is constituted by multiple components, each isolated within its own
package and each self documented in literate programming style.

The primary components are:

* The Compute Engine, in charge of executing analyses on the data.
* The Dataframe API, which provides an high level API for the compute engine.
* The Warehouse, which provides access to the catalog of datasets

For the user guide and code documentation of each component, refer to the
component itself.
"""

from . import compute

__all__ = ("compute",)
