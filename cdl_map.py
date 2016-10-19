import json
import rdflib
from rdflib.term import URIRef, BNode, Literal
from rdflib.namespace import Namespace, DC, DCTERMS, SKOS, RDF, FOAF


class CdlMap:
    MAP = Namespace('http://dp.la/about/map/')
    EDM = Namespace('http://www.europeana.eu/schemas/edm/')
    ORE = Namespace('http://www.openarchives.org/ore/terms/')

    def __init__(self):

        self.graph = rdflib.Graph()
        self.graph.bind('dc', DC)
        self.graph.bind('rdf', RDF)
        self.graph.bind('skos', SKOS)
        self.graph.bind('map', self.MAP)
        self.graph.bind('edm', self.EDM)
        self.graph.bind('ore', self.ORE)
        self.graph.bind('dcterms', DCTERMS)

    def serialize(self):
        return self.graph.serialize(format='turtle')

    def map(self, jsonFile):
        with (open(jsonFile)) as data_file:
            data = json.load(data_file)
            g = self.graph

            item = URIRef(data['url_item'])
            g.add((item, RDF.type, self.EDM['WebResource']))

            cdl = URIRef('http://dp.la/api/contributor/cdl')
            g.add((cdl, RDF.type, self.EDM['Agent']))
            g.add((cdl, SKOS['prefLabel'], Literal('California Digital Library')))

            thumb = URIRef('https://thumbnails.calisphere.org/clip/150x150/' + data['reference_image_md5'])
            g.add((thumb, RDF.type, self.EDM['WebResource']))

            root = BNode()
            g.add((root, RDF.type, self.ORE['Aggregation']))

            originalRecord = BNode()
            g.add((root, self.MAP['originalRecord'], originalRecord))
            g.add((originalRecord, RDF.type, self.EDM['WebResource']))

            aggregatedCHO = BNode()
            g.add((root, self.EDM.aggregatedCHO, aggregatedCHO))
            g.add((aggregatedCHO, RDF.type, self.MAP.SourceResource))

            if 'title_ss' in data:
                for title in data['title_ss']:
                    g.add((aggregatedCHO, DCTERMS.title, Literal(title)))

            if 'date_ss' in data:
                for date in data['date_ss']:
                    date_bnode = BNode()
                    g.add((aggregatedCHO, DC.date, date_bnode))
                    g.add((date_bnode, RDF.type, self.EDM.TimeSpan))
                    g.add((date_bnode, self.MAP.providedLabel, Literal(date)))

            if 'identifier_ss' in data:
                for identifier in data['identifier_ss']:
                    g.add((aggregatedCHO, DC.identifier, Literal(identifier)))

            g.add((aggregatedCHO, DC.identifier, Literal(data['url_item'])))

            if 'rights_ss' in data:
                for rights in data['rights_ss']:
                    g.add((aggregatedCHO, DC.rights, Literal(rights)))

            if 'contributor_ss' in data:
                for contributor in data['contributor_ss']:
                    contributor_bnode = BNode()
                    g.add((aggregatedCHO, DCTERMS.contributor, contributor_bnode))
                    g.add((contributor_bnode, RDF.type, self.EDM.Agent))
                    g.add((contributor_bnode, self.MAP.providedLabel, Literal(contributor)))

            if 'creator_ss' in data:
                for creator in data['creator_ss']:
                    creator_bnode = BNode()
                    g.add((aggregatedCHO, DCTERMS.creator, creator_bnode))
                    g.add((creator_bnode, RDF.type, self.EDM.Agent))
                    g.add((creator_bnode, self.MAP.providedLabel, Literal(creator)))

            if 'collection_name' in data:
                for collection in data['collection_name']:
                    collection_bnode = BNode()
                    g.add((aggregatedCHO, DCTERMS.isPartOf, collection_bnode))
                    g.add((collection_bnode, RDF.type, DCTERMS.Collection))
                    g.add((collection_bnode, DCTERMS.title, Literal(collection)))

            if 'publisher_ss' in data:
                for publisher in data['publisher_ss']:
                    g.add((aggregatedCHO, DCTERMS.publisher, Literal(publisher)))

            if 'type' in data:
                for type in data['type']:
                    g.add((aggregatedCHO, DCTERMS.type, Literal(type)))

            provider = None

            if 'campus_name' in data and 'repository_name' in data:
                provider = data['campus_name'][0] + ', ' + data['repository_name'][0]

            elif 'repository_name' in data:
                provider = data['repository_name'][0]

            if provider is not None:
                provider_bnode = BNode()
                g.add((root, self.EDM.dataProvider, provider_bnode))
                g.add((provider_bnode, RDF.type, self.EDM.Agent))
                g.add((provider_bnode, self.MAP.providedLabel, Literal(provider)))

            g.add((root, self.EDM.isShownAt, item))

            if 'reference_image_md5' in data:
                md5 = data['reference_image_md5']
                image_url = "https://thumbnails.calisphere.org/clip/150x150/" + md5
                g.add((root, self.EDM.preview, URIRef(image_url)))


if __name__ == "__main__":
    cdl = CdlMap()
    cdl.map('sample_data/0005e05a7ae83013c3c5c4272a99e944.json')
    print(cdl.serialize().decode("utf-8"))
