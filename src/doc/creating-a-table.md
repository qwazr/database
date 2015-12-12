# Creating a table

This page explains how to create a table in QWAZR.

## REST/JSON API

### HTTP request

- **URL**: http://{server_name}:9091/table/{table_name}
- **Path parameters**:
    - server_name: the name of the server
    - table_name: the name of the table
- **HTTP Method**: POST
- **HTTP Headers**:
    - Content-Type: application/json

### Request body

The body is a JSON object with a set of column descriptions.

Each item of the **columns** element describes a column of the table:

```json
{
  "columns": {
    "{column_name}": {
      "type": "{column_type}",
      "mode": "{column_mode}"
  }
}
```

**Example of body:**

```json
{
    "columns": {
        "category": { "type": "STRING", "mode": "INDEXED" },
        "title": {"type": "STRING", "mode": "STORED" },
        "price": {"type": "DOUBLE", "mode": "STORED" }
    }
}
```

## Column types

- **STRING**: UTF-8 characters
- **DOUBLE**: 64-bit IEEE 754 floating point
- **LONG**: 64-bit two's complement integer

## Column modes

The column mode defines if the value are indexed or stored:
- **STORED**: The value is only stored, the column cannot be used in a query.
- **INDEXED**: The content is stored and indexed. The column can be used in a query.


## Javascript API

_TO DO_

## Java API

_TO DO_