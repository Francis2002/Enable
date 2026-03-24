import osmium

class TagInspector(osmium.SimpleHandler):
    def __init__(self):
        super(TagInspector, self).__init__()
        self.refs = set()

    def way(self, w):
        if 'highway' in w.tags and w.tags['highway'] in ['motorway', 'motorway_link']:
            if 'ref' in w.tags:
                self.refs.add(w.tags['ref'])

inspector = TagInspector()
inspector.apply_file("../../data/01_raw/portugal-latest.osm.pbf")
print("Found refs:", sorted(list(inspector.refs))[:50])
