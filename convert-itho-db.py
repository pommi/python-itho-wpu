#!/usr/bin/env python3
#
# Convert the required data for python-itho-wpu from Itho Servicetool
# $_parameters_HeatPump.par (Microsoft Access database) to SQLite SQL.
#
# Dependencies: python3-pyodbc, mdbtools (>= 0.9.0), odbc-mdbtools (>= 0.9.0)

import argparse
import db
import os
import pyodbc
import re
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Convert Itho Servicetool database to SQLite",
    )
    parser.add_argument("--itho-db", nargs="?", required=True, help="Itho Database file")
    parser.add_argument(
        "--sqlite-db", nargs="?", default="heatpump.sqlite", help="Itho Database file"
    )
    parser.add_argument("--force", action="store_true", help="Force overwrite SQLite database")
    args = parser.parse_args()
    return args


def convert(par_file, sqlite_db):
    par_file = par_file.replace("$", "\\$")
    par_conn = pyodbc.connect(f"DRIVER={{MDBTools}};DBQ={par_file};")
    par_conn.setencoding("UTF-8")
    par_conn.setdecoding(pyodbc.SQL_CHAR, encoding="UTF-8")
    par_cur = par_conn.cursor()

    sqlite_db = db.sqlite(sqlite_db)

    tables = []
    for table_info in par_cur.tables(tableType="TABLE"):
        if re.match(
            "^(VersieBeheer|Data[Ll]abel|Parameterlijst|Handbed|Counters)", table_info.table_name
        ):
            tables.append(table_info.table_name)

    for t in sorted(tables):
        sqlite_db.create_table(t.lower())
        data = []
        if re.match("^Data[Ll]abel", t):
            par_cur.execute(f"select Index, Naam, Tekst_NL, Tooltip_NL, Eenheid_NL from {t}")
            rows = par_cur.fetchall()
            for r in sorted(rows):
                data.append((r.Index, r.Naam, r.Tekst_NL, r.Tooltip_NL, r.Eenheid_NL))
        if re.match("^Parameterlijst", t):
            par_cur.execute(
                "select Index, Naam, Naam_fabriek, Min, Max, Default, "
                f"Tekst_NL, Omschrijving_NL, Eenheid_NL from {t}"
            )
            rows = par_cur.fetchall()
            for r in sorted(rows):
                data.append(
                    (
                        r.Index,
                        r.Naam,
                        r.Naam_fabriek,
                        r.Min,
                        r.Max,
                        r.Default,
                        r.Tekst_NL,
                        r.Omschrijving_NL,
                        r.Eenheid_NL,
                    )
                )
        if re.match("^Counters", t):
            par_cur.execute(f"select Index, Naam, Tekst_NL, Tooltip_NL, Eenheid_NL from {t}")
            rows = par_cur.fetchall()
            for r in sorted(rows):
                data.append(
                    (
                        r.Index,
                        r.Naam,
                        r.Tekst_NL,
                        r.Tooltip_NL,
                        r.Eenheid_NL,
                    )
                )
        if re.match("^Handbed", t):
            par_cur.execute(
                "select Index, Naam, Naam_fabriek, Min, Max, Default, "
                f"Tekst_NL, Tooltip_NL, Eenheid_NL from {t}"
            )
            rows = par_cur.fetchall()
            for r in sorted(rows):
                data.append(
                    (
                        r.Index,
                        r.Naam,
                        r.Naam_fabriek,
                        r.Min,
                        r.Max,
                        r.Default,
                        r.Tekst_NL,
                        r.Tooltip_NL,
                        r.Eenheid_NL,
                    )
                )
        if re.match("^VersieBeheer", t):
            par_cur.execute(
                f"select VersieNummer, DataLabel, ParameterLijst, Handbed, Counters from {t}"
            )
            rows = par_cur.fetchall()
            for r in sorted(rows):
                data.append((r.VersieNummer, r.DataLabel, r.ParameterLijst, r.Handbed, r.Counters))
        sqlite_db.insert(t.lower(), data)


if __name__ == "__main__":
    args = parse_args()
    print(f"Converting {args.itho_db} to {args.sqlite_db} ...")
    if not os.path.exists(args.itho_db):
        print(f"Itho database does not exist: {args.itho_db}")
        sys.exit(1)
    if os.path.exists(args.sqlite_db):
        if args.force:
            print(f"Removing existing SQLite database: {args.sqlite_db}")
            os.remove(args.sqlite_db)
        else:
            print(f"Error: SQLite database exists: {args.sqlite_db}")
            sys.exit(1)
    convert(args.itho_db, args.sqlite_db)
