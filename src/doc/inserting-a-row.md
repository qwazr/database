# Inserting a row

This page describes how to insert/update a row in a table.

If the row does not exist, the row is created (upsert).

## REST/JSON API

### HTTP request

- **URL**: http://{server_name}:9091/table/{table_name}/row/{row_id}
- **Path parameters**:
    - server_name: the name of the server
    - table_name: the name of the table
    - row_id: the unique ID of the row
- **Method**: PUT
- **Headers**:
    - Content-Type: application/json

### The body of the request:

The body is a JSON object which describe the row.

**Example of row:**

```json
{
    "title": "A beautiful day",
    "category": [ "news", "archive"],
    "price": 24.99
}
```

_The database supports several values for one column on indexed columns._

The format for each columns is:
- Insert one value: "{columns_name}": "{value}"
- Insert an array of value: "{column_name}: ["value1", "value2", "value3"]


## Javascript API

_TO DO_

## Java API

_TO DO_