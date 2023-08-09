# Table-Differ: Multi-Database Supported Table Comparison Utility

# Overview <a name="overview"></a>
Table Differ is a table comparison utility that aims to assist in validating changes made to large tables within several supported databases.
Table Differ provides a table containing the changed and modified rows between two large tables called diff_table. This diff_table is then
used to perform simple reporting on the type of changes that were made between the two tables. The primary goal of the Table Differ is to be as maleable
as possible in order to be usable in as many situations as possible.

Table Differ is not necessarily intented to be used on a history of two tables, but rather to be used on any two tables with an emphasis on being able to accept
any number of key columns to track rows by, along with any number of comparison columns to look for differences within. The columns to compare can also be supplied
into the Table Differ by way of 'ignored columns' where any column besides the ones specified will be looked at for when dealing with wide tables.

As of right now the following databases supported by Table Differ are:
- **SQLite**

The following databases are in the process of being supported:
- **PostgreSQL** (nearly complete)
- **MySql**
- **DuckDB**



# TABLE OF CONTENTS
1. ### [Overview](#overview)
2. ### [Usage](#usage_contents)
  - Installation
  - Running Table Differ
    - Connecting to a Database
    - Arguments and Configs
    - Use Cases of Table Differ
  - diff_table capabilities
3. ### [Credits](#credits_contents)
4. ### [License](#license_contents)
---


# Usage <a name="usage_contents"></a>
## Installation
**TABLE DIFFER IS CURRENTLY BEING PACKAGED USING PYPI BUT IS NOT CURRENTLY FINISHED, PLEASE HOLD**
## Running Table Differ
While running Table Differ is very straightforward, there are a few important notes to understand about how it works.

1. Table Differ will attempt to create a new table within the database that it is pulling the two comparison tables from. Depending on your pipeline this could potentially cause issues.

2. When Table Differ runs, it will attempt to drop any table it can find named 'diff_table' before it creates it's version of the table.

**IF YOU HAVE A TABLE WITHIN YOUR DATABASE NAMED diff_table, IT WILL BE DROPPED BY TABLE DIFFER**

3. After Table Differ is finished running, it does not drop the table that it has created within your database, and only drops that table at the beginning of the next run so that the diff table can be queried if needed between usages.

### Connecting to a Database
### Arguments and Configs
The developement of Table Differ focuses on making a product that is as flexible and adaptable as possible, and because of this uses several potential arguments.

**Required Arguments**
|**Argument**     |Description|
|---------------------------|-----------|
|-c --comparison_columns    |columns to be specifically compared (columns not specified will **not** be looked at when creating the diff_table). This field takes n number of arguments. |
|**OR**                     |
| -i --ignore_columns       |columns to be specifically ignored (columns not specified **will** be looked at when creating the diff_table). This field takes n number of arguments. |


**Semi-Required Arguments** (arguments that are needed unless specified through the --configs argument)
|**Argument**|Description|Choices|Default|
|---------------------------|-----------|-------|-------|
|--configs                  |This exists as an option to supply the db_type, table names, and key columns, through a config.yaml file instead of as arguments. A 'y' will use the yaml file.|y, n|n|
|--db_type                  |The type of database that Table Differ will attempt to connect to.|sqlite, postgres, mysql, duckdb||
|--tables                   |Tables by name to be used in comparison and creation of diff_table. The first name will always be the initial table, and the latter will be the secondary table. This field takes 2 arguments| | |
|--key_columns              |Key Columns to track rows by within **both** tables. This field takes n number of arguments| | |


**Optional Arguments**
|**Argument**|Description|Choices|Default|
|----------------------|-----------|-------|-------|
|--except_rows         |Rows to be excluded from the creation of diff_table by their key. This exists in case you would like to skip over a known outlier. This field take n number of arguments.| | |
|--logging_level       |Sets the logging level of Table Differ.|debug, info, warning, error, critical|warning|
|--print_tables        |Prints the tables used in the creation of the diff_table to the console using [rich](https://rich.readthedocs.io/en/stable/introduction.html). This is primarily useful in testing and usages on very small tables.|y, n|n|

**Configs** (stored within the configs.yaml file)
|**Config Name**|Description|
|---------------|-----------|
|db_host|Hosting URL for database being used.|
|db_port|Port number for database connection.|
|db_name|Name of database being connected to.|
|db_user|Username of user for database.|
|db_type|Type of database being connected to.|
|table_initial|Name of the first table that will be used to create the diff_table. This can be supplied either here in the config file or as an argument if the --configs argument is set to 'n'.|
|table_secondary|Name of the second table that will be used to create the diff_table. This can be supplied either here in the config file or as an argument if the --configs argument is set to 'n'.|
|key_columns|Key columns that Table Differ will query the selected tables by. This can be supplied either here in the config file or as an argument if the --configs argument is set to 'n'. This accepts n number of fields|
|initial_table_name|Placeholder name of the first table being queried in creation of the diff_table. Default is set to 'origin'.|
|secondary_table_name|Placeholder name of the second table being queried in creation of the diff_table.|

### Use Cases of Table Differ
While Table Differ obviously works very well at comparing a history of a single table, it is not limited to just that. Because of the emphasis on flexibility and usability, Table Differ is designed to be used in any case where you need to see the specific differences between two tables within a database.

## diff_table capabilities
Rows that are included into the diff_table have to meet specific conditions:
1. Rows that have a changed value from the initial to secondary table
2. Rows that exist in the initial table but not in the secondary
3. Rows that exist in the secondary table but not in the initial
Note that all rows are compared based on given key columns, and only columns specified (or in the case of ignored columns, not specified) will be looked into for changed.
If a change on a row exists but in a column not specified, it will not be added to the diff_table based on that change.

# CREDITS <a name="credits_contents"></a>
1. Benjamin Farmer - San Juan Data LLC. (2023)

# License <a name="license_contents"></a>
*Copyright (c) [2023] [Benjamin Farmer]*

*Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:*

*The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.*

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
