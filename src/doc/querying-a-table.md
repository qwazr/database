# Querying a table

This page describes how to retrieve rows from the table using queries.

## REST JSON/API

### HTTP request

- **URL**: http://{server_name}:9091/table/{table_name}/query
- **Path parameters**:
    - server_name: the name of the server
    - table_name: the name of the table
- **Method**: POST
- **Headers**:
    - Content-Type: application/json

### Request body

The body is a JSON objects which describe a query.

**An example of query:**

```json
{
    "start": 0,
    "rows": 10,
    "columns": ["title", "category"],
    "counters": ["category"],
    "query": {
        "$AND": [
            {
                "$OR": [
                    {"category": "news"},
                    {"category": "other"}
                ]
            },
            {
                "category": "archive"
            }
        ]
    }
}
```


## Javascript API

_TO DO_

## Java API

_TO DO_