class DataSerializer(object):
    

    def __init__(self):
        pass


    def serialize_content_based(self, item):
        return {
            'title': item['title'],
            'genres': item['genres'],
            'score': item['score']
        }
    

    def serialize_collaborative(self, item):
        return {
            'title': item['title'],
            'score': item['score']
        }
