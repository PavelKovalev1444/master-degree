class DataSerializer(object):
    

    def __init__(self):
        pass


    def serialize_content_based(self, item):
        return {
            'title': item['title'],
            'genres': list(set(item['genres'])),
            'score': item['score']
        }
    
    
    def serialize_content_based_raw(self, item):
        return {
            'title': item['title'],
            'ux_classifiers': item['ux_classifiers'],
            'ux_features': item['ux_features'],
            'classifiers': item['classifiers'],
            'features': item['features'],
            'genres': list(set(item['genres'])),
        }
    

    def serialize_collaborative_similar_users(self, item):
        return {
            'user': {
                'id': item['user']['id'],
                'name': item['user']['name'],
            },
            'similar': {
                'id': item['similar']['id'],
                'name': item['similar']['name'],
            },
            'ux_classifiers': item['ux_classifiers'],
            'ux_features': item['ux_features'],
            'classifiers': item['classifiers'],
            'features': item['features'],
        }
    

    def serialize_collaborative(self, item):
        return {
            'title': item['title'],
            'ux_classifiers': item['ux_classifiers'],
            'ux_features': item['ux_features'],
            'classifiers': item['classifiers'],
            'features': item['features'],
            'genres': list(set(item['genres'])),
        }
