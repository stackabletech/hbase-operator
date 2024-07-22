import base64
import http
import requests
import xml.etree.ElementTree as ET
import sys


class HbaseClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "text/xml",
                "Content-Type": "text/xml",
            }
        )
        http.client.HTTPConnection.debuglevel = 1

    @staticmethod
    def encode_value(plain_string):
        return base64.b64encode(plain_string.encode("utf-8")).decode("ascii")

    @staticmethod
    def decode_value(base64_string):
        return base64.b64decode(base64_string).decode("utf-8")

    def create_table(self, rest_url, name, column_family, compression):
        response = self.session.put(
            f"{rest_url}/{name}/schema",
            data=f"""
                    <TableSchema name="{name}">
                        <ColumnSchema name="{column_family}" COMPRESSION="{compression}" />
                    </TableSchema>
                """,
        )
        assert response.status_code == 201
        table_schema_location = response.headers["location"]
        table_location = table_schema_location.removesuffix("/schema")
        return (table_location, table_schema_location)

    def delete_table(self, table_schema_location):
        response = self.session.delete(table_schema_location)
        assert response.status_code == 200

    def put_row(self, table_location, row_key, column_family, column, cell_value):
        cell_column = f"{column_family}:{column}"
        response = self.session.put(
            f"{table_location}/{row_key}",
            data=f"""
                    <CellSet>
                        <Row key="{self.encode_value(row_key)}">
                            <Cell column="{self.encode_value(cell_column)}">
                                {self.encode_value(cell_value)}
                            </Cell>
                        </Row>
                    </CellSet>
                """,
        )
        assert response.status_code == 200

    def put_scanner(self, table_location):
        response = self.session.put(
            f"{table_location}/scanner", data='<Scanner batch="1" />'
        )
        assert response.status_code == 201
        return response.headers["location"]

    def get_scanner(self, scanner_location):
        response = self.session.get(scanner_location)
        assert response.status_code == 200
        return response.text

    def delete_scanner(self, scanner_location):
        response = self.session.delete(scanner_location)
        assert response.status_code == 200


hbase_rest_url = sys.argv[1]

hbase = HbaseClient()

# supported compression algorithms: SNAPPY, GZ
compression_opts = ["NONE", "SNAPPY", "GZ"]

for compression in compression_opts:
    print(f"""
    Create a table with compression={compression}
    ==============""")
    column_family = "cf"
    (table_location, table_schema_location) = hbase.create_table(
        rest_url=hbase_rest_url,
        name="companies",
        column_family=column_family,
        compression=compression,
    )

    print("""
    Write a row to the table
    ========================""")
    cell_value = "Stackable GmbH"
    hbase.put_row(
        table_location=table_location,
        row_key="stackable",
        column_family=column_family,
        column="name",
        cell_value=cell_value,
    )

    print("""
    Get a scanner object
    ====================""")
    scanner_location = hbase.put_scanner(table_location)

    print("""
    Get the next batch from the scanner
    ===================================""")
    scan = hbase.get_scanner(scanner_location)

    print("""
    Verify table content
    ====================""")
    parser = ET.fromstring(scan)
    actual_cell_value = hbase.decode_value(parser.findtext("./Row/Cell"))
    print(f'assert "{actual_cell_value}" == "{cell_value}"')
    assert actual_cell_value == cell_value

    print("""
    Delete the scanner
    ==================""")
    hbase.delete_scanner(scanner_location)

    print("""
    Delete the table
    ================""")
    hbase.delete_table(table_schema_location)
