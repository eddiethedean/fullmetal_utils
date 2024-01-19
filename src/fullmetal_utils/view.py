"""
View objects are similar to Table objects, except that any attempts to insert 
or update data will throw an error. The full list of methods and properties
available on a view object is as follows:

columns
columns_dict
count
schema
rows
rows_where(where, where_args, order_by, select)
drop()
"""

class View:
    ...