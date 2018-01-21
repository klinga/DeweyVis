# imports bibliographic and item data into sqlite database (files/datastore.db)

import unicodecsv as csv

from datastore import (Bib, Item, session_scope, insert_or_ignore,
                       Bstatus, Country, Lang, CatSource, CatAgency,
                       Istatus, Location)


fh = './files/bpl_171220_raw.csv'
with open(fh, 'r') as csvfile:
    reader = csv.reader(csvfile, encoding='utf-8')
    print list(enumerate(reader.next()))

    for row in reader:
    # for x in range(5):
        # row = reader.next()
        with session_scope() as session:
            bstatus = dict(
                code=row[3])
            bstatus = insert_or_ignore(session, Bstatus, **bstatus)
            country = dict(
                code=row[6])
            country = insert_or_ignore(session, Country, **country)
            lang = dict(
                code=row[9])
            lang = insert_or_ignore(session, Lang, **lang)
            catSrc = dict(
                code=row[8])
            catSrc = insert_or_ignore(session, CatSource, **catSrc)
            catAgn = dict(
                code=row[10])
            catAgn = insert_or_ignore(session, CatAgency, **catAgn)

            session.flush()

            bib = dict(
                bid=row[0],
                dateCreated=row[1],
                dateCataloged=row[2],
                bsid=bstatus.bsid,
                itemCount=row[4],
                callNo=row[5],
                cid=country.cid,
                pubDate=row[7],
                laid=lang.laid,
                csid=catSrc.csid,
                caid=catAgn.caid)
            insert_or_ignore(session, Bib, **bib)

            iStatus = dict(
                code=row[13])
            iStatus = insert_or_ignore(session, Istatus, **iStatus)
            loc = dict(
                code=row[14])
            loc = insert_or_ignore(session, Location, **loc)

            session.flush()

            item = dict(
                iid=row[11],
                bid=row[0],
                dateCreated=row[12],
                isid=iStatus.isid,
                loid=loc.loid,
                checkout=row[15],
                renewal=row[16],
                lastCheckout=row[17])

            insert_or_ignore(session, Item, **item)
