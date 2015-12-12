# Retrieving a row

This page describes how to retrieve a row using its id.

## REST/JSON API

### HTTP request

- **URL**: http://{server_name}:9091/table/{table_name}/row/{row_id}?column={column_name1}&column={column_name2}...
- **Path parameters**:
    - server_name: the name of the server
    - table_name: the name of the table
    - row_id: the ID of the row
- **Query parameters**:
    - column: the name of the column.
- **Method**: GET

### Example:

    http://localhost:9091/table/test/row/8?column=title&column=price&column=category

The returned JSON:

```json
{
  "price": [ 24.99 ],
  "title": [ "A beautiful day" ],
  "category": [ "news", "archive" ]
}
```

## Javascript API

_TO DO_

## Java API

_TO DO_