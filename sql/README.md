# KitDB SQL

What the PostgreSQL port (`pgwire`) reads statements with:

- `Lex`: the bounded lexer, tokens with their byte spans in the statement (1 MiB, 4,096 tokens at most);
- PostgreSQL's type identifiers (OIDs) and type names;
- `ParseUUID` and `FormatUUID`.

The parser that read whole statements (KROW's SQL, about 5,000 lines) went on 11/11/2026, when KitDB left the first
engine for its own module. Its last use was telling an insert, an update or a delete that returns rows from one that
does not; `pgwire` scans the tokens for that now (`returnsRows`), checked against the parser on the 345 statements of
the module's tests it could read before it went.
