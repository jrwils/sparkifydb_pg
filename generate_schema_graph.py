from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData


def main():
    graph = create_schema_graph(
        metadata=MetaData('postgresql://student:student@127.0.0.1/sparkifydb')
    )
    # I am writing to .svg here, since write_png gives poor image quality.
    # I then save it to a .png file to be displayed in README.md.
    graph.write_svg('sparkifydb_erd.svg')


if __name__ == "__main__":
    main()
