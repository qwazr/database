# Inserting a batch of rows

This page describes how to insert a set of rows in a table.

## HTTP request

- **URL**: http://{server_name}:9091/table/{table_name}/row?buffer={buffer_size}
- **Path parameters**:
    - server_name: the name of the server
    - table_name: the name of the table
- **Query parameters**:
    - buffer_size: the size of the buffer. The default value is 50.
- **Method**: POST
- **Headers**:
    - Content-Type: text/plain

## Request body

The body is a set of JSON objects separated by a line feed.

In this case, the body is not a valid JSON structure (that's why we use the text/plain mime type).

**An example of batch:**

```json
{ "_id": "1", "title": "New release", "category": "news" }
{ "_id": "2", "title": "Distributed computing is great", "category": "news" }
{ "_id": "3", "title": "Any product", "category": "product", "price": 12 }
{ "_id": "6", "title": "More is best", "category": ["archive", "news"] }