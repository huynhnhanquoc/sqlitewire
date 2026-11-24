// Package sql holds what KitDB's PostgreSQL port reads statements with: the bounded lexer (tokens and their byte
// spans), PostgreSQL's type identifiers (OIDs) and the text form of a UUID. The parser that read whole statements went
// on 11/11/2026, when KitDB left the first engine for its own module: its last use, telling an insert, an update or a
// delete that returns rows from one that does not, is a scan of the tokens (pgwire, returnsRows).
package sql
