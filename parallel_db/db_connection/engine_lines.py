import sqlalchemy

sqlalchemy_prefixes = {
    "oracle": "oracle+cx_oracle://",
    "pl": "oracle+cx_oracle://",
    "sqlite": "sqlite:///",
    "mssql": "mssql+pynssql://",
    "msodbc": "mssql+pyodbc://",
}