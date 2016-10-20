import json
import rdflib
from rdflib.term import URIRef, BNode, Literal
from rdflib.namespace import Namespace, DC, DCTERMS, SKOS, RDF, FOAF


class CdlMap:

    @staticmethod
    def map(raw_data):
        MAP = Namespace('http://dp.la/about/map/')
        EDM = Namespace('http://www.europeana.eu/schemas/edm/')
        ORE = Namespace('http://www.openarchives.org/ore/terms/')

        g = rdflib.Graph()
        g.bind('dc', DC)
        g.bind('rdf', RDF)
        g.bind('skos', SKOS)
        g.bind('map', MAP)
        g.bind('edm', EDM)
        g.bind('ore', ORE)
        g.bind('dcterms', DCTERMS)

        data = json.load(raw_data)

        item = URIRef(data['url_item'])
        g.add((item, RDF.type, EDM['WebResource']))

        cdl = URIRef('http://dp.la/api/contributor/cdl')
        g.add((cdl, RDF.type, EDM['Agent']))
        g.add((cdl, SKOS['prefLabel'], Literal('California Digital Library')))

        if 'reference_image_md5' in data:
            thumb = URIRef('https://thumbnails.calisphere.org/clip/150x150/' + data['reference_image_md5'])
            g.add((thumb, RDF.type, EDM['WebResource']))

        root = BNode()
        g.add((root, RDF.type, ORE['Aggregation']))

        originalRecord = BNode()
        g.add((root, MAP['originalRecord'], originalRecord))
        g.add((originalRecord, RDF.type, EDM['WebResource']))

        aggregatedCHO = BNode()
        g.add((root, EDM.aggregatedCHO, aggregatedCHO))
        g.add((aggregatedCHO, RDF.type, MAP.SourceResource))

        if 'title_ss' in data:
            for title in data['title_ss']:
                g.add((aggregatedCHO, DCTERMS.title, Literal(title)))

        if 'date_ss' in data:
            for date in data['date_ss']:
                date_bnode = BNode()
                g.add((aggregatedCHO, DC.date, date_bnode))
                g.add((date_bnode, RDF.type, EDM.TimeSpan))
                g.add((date_bnode, MAP.providedLabel, Literal(date)))

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
                g.add((contributor_bnode, RDF.type, EDM.Agent))
                g.add((contributor_bnode, MAP.providedLabel, Literal(contributor)))

        if 'creator_ss' in data:
            for creator in data['creator_ss']:
                creator_bnode = BNode()
                g.add((aggregatedCHO, DCTERMS.creator, creator_bnode))
                g.add((creator_bnode, RDF.type, EDM.Agent))
                g.add((creator_bnode, MAP.providedLabel, Literal(creator)))

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
            g.add((root, EDM.dataProvider, provider_bnode))
            g.add((provider_bnode, RDF.type, EDM.Agent))
            g.add((provider_bnode, MAP.providedLabel, Literal(provider)))

        g.add((root, EDM.isShownAt, item))

        if 'reference_image_md5' in data:
            md5 = data['reference_image_md5']
            image_url = "https://thumbnails.calisphere.org/clip/150x150/" + md5
            g.add((root, EDM.preview, URIRef(image_url)))

        return g.serialize(format='turtle')


if __name__ == "__main__":

    with (open('sample_data/0005e05a7ae83013c3c5c4272a99e944.json')) as data_file:
        output = CdlMap.map(data_file)
        print(output.decode("utf8"))
