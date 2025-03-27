with db.session() as session:
        query = '''
            LOAD CSV WITH HEADERS FROM "file:///home/pavel/repos/master-degree/app/tmp/nodes_movies.csv" AS row
            CREATE (:Movie {
                title: row.title,
                Format: row.Format,
                ReleaseYear: toInteger(row.ReleaseYear),
                MPAA_Rating: row.MPAA_Rating,
                Price: toFloat(row.Price)
            })
        '''
        session.run(query)

# MATCH (n) DETACH DELETE n
