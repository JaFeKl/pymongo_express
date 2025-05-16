# PymongoExpress
A utility package to make working with pymongo even easier

# Getting Started
The package uses a single class named `GraphDB`. To use the interface, simply generate an object from this class:

```python
from pymongo_express import Client

myDB = Client(
    url=<url_of_the_db_instance>,
    username=<your_username>
    password=<your_password>
    port=<port_of_the_db_instance>
)
```

The `Client` gives access to many simplified CRUD operations for you mongo DB.


