class DataSerializer(object):
    

    def __init__(self):
        pass


    def serialize_movie(self, movie):
        return {
            "format": movie["format"],
            "price": movie["price"],
            "releaseYear": movie["releaseYear"],
            "title": movie["title"],
            "mpaa_rating": movie["mpaa_rating"],
        }
