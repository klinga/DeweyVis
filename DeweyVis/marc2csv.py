from pymarc import MARCReader
import unicodecsv as csv


def save2csv(file, row):
    with open(file, 'a') as csvfile:
        writer = csv.writer(
            csvfile,
            encoding='utf-8',
            delimiter=',',
            quotechar='"', quoting=csv.QUOTE_MINIMAL,
            lineterminator='\n')
        writer.writerow(row)


def marc2csv(fh_in, fh_out):
    # heading row
    head = [
        'bId', 'bDateCreated', 'bDateCataloged',
        'bStatus', 'bItemCount',
        'callNo', 'pCountry', 'pDate', 'catSource',
        'lang', 'catAgent',
        'iId', 'iDateCreated', 'iStatus', 'iLocation',
        'iCheckout', 'iRenewal', 'iLastCheckoutDate']
    save2csv(fh_out, head)

    with open(fh_in, 'rb') as marcfile:
        reader = MARCReader(marcfile, hide_utf8_warnings=True)
        counter = 0
        skipped_bibs = 0
        for record in reader:
            counter += 1
            try:
                bId = record['907']['a'][2:-1]
                bDateCreated = record['907']['c']
                bDateCataloged = record['907']['b']
                bStatus = record['998']['e']
                bItemCount = record['998']['i']
                if '099' in record:
                    callNo = record['099'].value()
                else:
                    callNo = None
                if '008' in record:
                    pCountry = record['008'].value()[15:18]
                    pDate = record['008'].value()[07:11]
                    catSource = record['008'].value()[39]
                    lang = record['008'].value()[35:38]
                else:
                    pCountry = None
                    pDate = None
                    catSource = None
                    lang = None
                if '040' in record:
                    catAgent = record['040']['a']
                else:
                    catAgent = None
                if '945' in record:
                    items = record.get_fields('945')
                    for item in items:
                        if item.indicators == [' ', ' ']:
                            iId = item['y'][2:-1]
                            iDateCreated = item['z']
                            iStatus = item['s']
                            iLocation = item['l']
                            iCheckout = item['u']
                            iRenewal = item['v']
                            iLastCheckoutDate = item['k']
                            row = [
                                bId, bDateCreated, bDateCataloged,
                                bStatus, bItemCount,
                                callNo, pCountry, pDate, catSource,
                                lang, catAgent,
                                iId, iDateCreated, iStatus, iLocation,
                                iCheckout, iRenewal, iLastCheckoutDate
                            ]
                            save2csv(fh_out, row)

            except Exception as e:
                skipped_bibs += 1
                print 'bib:', counter, e

    print 'found {} bibs in file'.format(counter)
    print 'skipped {} bibs because of error'.format(
        skipped_bibs)


if __name__ == '__main__':
    import sys

    fh_in = sys.argv[1]
    fh_out = sys.argv[2]

    # extract data from marc and output specified elements in
    # csv file
    marc2csv(fh_in, fh_out)
