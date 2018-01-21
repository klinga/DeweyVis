# imports bibliographic and item data into sqlite database (files/datastore.db)

import unicodecsv as csv

from datastore import Bib, Item, session_scope, insert_or_ignore


fh = './files/bpl_171220_raw.csv'
with open(fh, 'r') as csvfile:
    reader = csv.reader(csvfile, encoding='utf-8')
    print list(enumerate(reader.next()))

    for row in reader:
    # for x in range(5):
        # row = reader.next()
        with session_scope() as session:
            bib = dict(
                bid=row[0],
                dateCreated=row[1],
                dateCataloged=row[2],
                status=row[3],
                itemCount=row[4],
                callNo=row[5],
                country=row[6],
                pubDate=row[7],
                lang=row[8],
                catSource=row[9],
                catAgency=row[10])
            insert_or_ignore(session, Bib, **bib)

            item = dict(
                iid=row[11],
                bid=row[0],
                dateCreated=row[12],
                status=row[13],
                location=row[14],
                checkout=row[15],
                renewal=row[16],
                lastCheckout=row[17])

            insert_or_ignore(session, Item, **item)
