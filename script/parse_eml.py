from pathlib import Path
import datetime as dt
import xmltodict
import pandas as pd

DATA_FOLDER = Path('../data/')


def read_eml(path: Path):

    """Convert EML file to dictionary"""

    try:
        return xmltodict.parse(path.read_text(encoding="utf-8"))

    except UnicodeDecodeError as e:
        print(path.name)
        print(e)
        return None


def get_id_and_date():

    """Get identifier and date of election"""

    paths = SOURCE.glob('**/Verkiezingsdefinitie*.xml')
    for path in paths:

        data = read_eml(path)
        election = (data['EML']
                        ['ElectionEvent']
                        ['Election'])
        identifier = election['ElectionIdentifier']['@Id']
        date = election['ElectionIdentifier']['kr:ElectionDate']
        return (identifier, date)


def parse_election_data(data):

    """Parse election data and store results per station as CSV"""

    if pd.isnull(data):
        return []

    rows_aggregates = []
    rows_per_candidate = []
    rows_turnout = []

    contest = (data['EML']
                   ['Count']
                   ['Election']
                   ['Contests']
                   ['Contest'])
    
    try:
        contest_name = (data['EML']
                            ['Count']
                            ['Election']
                            ['ElectionIdentifier']
                            ['ElectionName'])
    except KeyError:
        contest_name = None

    try:
        if not contest_name:
            contest_name = contest['ContestIdentifier']['ContestName']
    except KeyError:
        contest_name = None

    try:
        managing_authority = (data['EML']
                                  ['ManagingAuthority']
                                  ['AuthorityIdentifier']
                                  ['#text'])
    except KeyError:
        managing_authority = None

    
    # base row template
    item = {
        'contest_name': contest_name,
        'managing_authority': managing_authority
    }

    # total row template
    total_item = item.copy()
    total_item["station_id"] = "TOTAL"
    total_item["station_name"] = "TOTAL"

    # extract top level totals
    total = contest["TotalVotes"]
    try:
        results = total['Selection']
    except TypeError:
        results = []

    # party votes
    row_aggregate = total_item.copy()
    for result in results:
        if 'AffiliationIdentifier' in result.keys():
            party_name = result['AffiliationIdentifier']['RegisteredName']
            party_id = result['AffiliationIdentifier']['@Id']
            if pd.isnull(party_name):
                party_name = party_id
            votes = int(result['ValidVotes'])
            row_aggregate[party_name] = votes

        # candidate votes
        elif PER_CANDIDATE.lower() in ['y', 'yes']:
            row_cand = total_item.copy()
            row_cand['party_name'] = party_name
            row_cand['party_id'] = party_id
            candidate_id = (result['Candidate']
                                    ['CandidateIdentifier']
                                    ['@Id'])
            row_cand['candidate_identifier'] = candidate_id
            row_cand['votes'] = result['ValidVotes']
            rows_per_candidate.append(row_cand)
    
    rows_aggregates.append(row_aggregate)

    # turnout, counted, rejected, invalid votes
    if TURNOUT.lower() in ['y', 'yes']:
        turnout_row = total_item.copy()
        
        try:
            turnout_row["cast"] = int(total["Cast"])
            turnout_row["counted"] = int(total["TotalCounted"])

            try:
                for rejected in total["RejectedVotes"]:
                    turnout_row["rejected: " + rejected["@ReasonCode"]] = int(rejected["#text"])
            except KeyError:
                pass
                    
            try:
                for uncounted in total["UncountedVotes"]:
                    turnout_row["uncounted: " + uncounted["@ReasonCode"]] = int(uncounted["#text"])
            except KeyError:
                pass

            rows_turnout.append(turnout_row)
        
        except TypeError:
            pass

    # start per station votes
    try:
        stations = contest['ReportingUnitVotes']
    except KeyError:
        stations = []
        rows_aggregates = [{
            'contest_name': contest_name,
            'managing_authority': managing_authority,
            'station_name': None,
            'station_id': None
        }]
        

    for station in stations:
        row_item = item.copy()
        
        try:
            row_item['station_id'] = station['ReportingUnitIdentifier']['@Id']
        except TypeError:
            row_item['station_id'] = None

        try:
            row_item['station_name'] = station['ReportingUnitIdentifier']['#text']
        except TypeError:
            row_item['station_name'] = None

        try:
            results = station['Selection']
        except TypeError:
            results = []

        row_aggregate = row_item.copy()
        for result in results:

            if 'AffiliationIdentifier' in result.keys():
                party_name = result['AffiliationIdentifier']['RegisteredName']
                party_id = result['AffiliationIdentifier']['@Id']
                if pd.isnull(party_name):
                    party_name = party_id
                votes = int(result['ValidVotes'])
                row_aggregate[party_name] = votes

            elif PER_CANDIDATE.lower() in ['y', 'yes']:
                row_cand = item.copy()
                row_cand['party_name'] = party_name
                row_cand['party_id'] = party_id
                candidate_id = (result['Candidate']
                                      ['CandidateIdentifier']
                                      ['@Id'])
                row_cand['candidate_identifier'] = candidate_id
                row_cand['votes'] = result['ValidVotes']
                rows_per_candidate.append(row_cand)
        
        rows_aggregates.append(row_aggregate)


        if TURNOUT.lower() in ['y', 'yes']:
            turnout_row = row_item.copy()
            
            try:
                turnout_row["cast"] = int(station["Cast"])
                turnout_row["counted"] = int(station["TotalCounted"])

                try:
                    for rejected in station["RejectedVotes"]:
                        turnout_row["rejected: " + rejected["@ReasonCode"]] = int(rejected["#text"])
                except KeyError:
                    pass
                        
                try:
                    for uncounted in station["UncountedVotes"]:
                        turnout_row["uncounted: " + uncounted["@ReasonCode"]] = int(uncounted["#text"])
                except KeyError:
                    pass

                rows_turnout.append(turnout_row)
            
            except TypeError:
                pass


    return (rows_aggregates, rows_per_candidate, rows_turnout, contest_name, managing_authority)


def process_files():

    """Process data files with local election results"""

    identifier, date = get_id_and_date()

    paths = SOURCE.glob('**/Telling*_*.xml')
    paths = [p for p in paths if 'kieskring' not in str(p).lower()]

    for path in paths:

        name = path.name.split('.')[0]

        data = read_eml(path)
        (rows_aggregates, rows_per_candidate, rows_turnout, contest_name, 
            managing_authority) = parse_election_data(data)

        if PER_CANDIDATE.lower() in ['y', 'yes']:
            filename = '{} per candidate.csv'.format(name)
            df = pd.DataFrame(rows_per_candidate)
            df.to_csv(str(VOTE_COUNTS / filename), index=False, encoding="utf-8")

        if TURNOUT.lower() in ['y', 'yes']:
            filename = '{} turnout.csv'.format(name)
            df = pd.DataFrame(rows_turnout)
            df.to_csv(str(VOTE_COUNTS / filename), index=False, encoding="utf-8")

        filename = '{} aggregate.csv'.format(name)
        df = pd.DataFrame(rows_aggregates)
        first_cols = [
            'contest_name', 'managing_authority',
            'station_name', 'station_id'
            ]
        columns = first_cols + [c for c in df.columns if c not in first_cols]
        df = df[columns]
        df.to_csv(str(VOTE_COUNTS / filename), index=False, encoding="utf-8")


def parse_candidates(data, rows):

    """Get candidate details"""

    election = (data['EML']
                    ['CandidateList']
                    ['Election'])
    try:
        contest_name = (election['Contest']
                                ['ContestIdentifier']
                                ['ContestName'])
    except KeyError:
        contest_name = None

    try:
        election_name = election['ElectionIdentifier']['ElectionName']
    except KeyError:
        election_name = None

    parties = election['Contest']['Affiliation']
    for party in parties:
        party_name = party['AffiliationIdentifier']['RegisteredName']
        party_id = party['AffiliationIdentifier']['@Id']

        party_candidates = party['Candidate']
        for candidate in party_candidates:

            try:
                identifier = candidate['CandidateIdentifier']['@Id']
            except TypeError:
                identifier = None

            if not isinstance(candidate, dict):
                continue

            for key in candidate['CandidateFullName'].keys():
                if key.startswith('ns'):
                    nr = key[2]
                    break

            try:
                first_name = (candidate['CandidateFullName']
                                       ['ns{}:PersonName'.format(nr)]
                                       ['ns{}:FirstName'.format(nr)])
            except (TypeError, KeyError):
                first_name = None

            try:
                last_name = (candidate['CandidateFullName']
                                      ['ns{}:PersonName'.format(nr)]
                                      ['ns{}:LastName'.format(nr)])
            except TypeError:
                last_name = None

            try:
                initials = (candidate['CandidateFullName']
                                     ['ns{}:PersonName'.format(nr)]
                                     ['ns{}:NameLine'.format(nr)]
                                     ['#text'])
            except TypeError:
                initials = None

            try:
                prefix = (candidate['CandidateFullName']
                                   ['ns{}:PersonName'.format(nr)]
                                   ['ns{}:NamePrefix'.format(nr)])
            except (TypeError, KeyError):
                prefix = None

            try:
                gender = candidate['Gender']
            except (TypeError, KeyError):
                gender = None

            try:
                address = (candidate['QualifyingAddress']
                                    ['ns{}:Locality'.format(nr)]
                                    ['ns{}:LocalityName'.format(nr)])
            except (TypeError, KeyError):
                address = None

            rows.append({
                'contest_name': contest_name,
                'election_name': election_name,
                'party_name': party_name,
                'party_id': party_id,
                'candidate_identifier': identifier,
                'first_name': first_name,
                'last_name': last_name,
                'initials': initials,
                'prefix': prefix,
                'gender': gender,
                'address': address
            })

    return rows


def create_candidate_list():

    """Create list with candidate details"""

    rows = []

    paths = SOURCE.glob('**/Kandidatenlijsten_*.xml')
    for p in paths:
        data = read_eml(p)
        if pd.notnull(data):
            rows = parse_candidates(data, rows)

    df = pd.DataFrame(rows)

    path = TARGET / 'candidates.csv'
    df.to_csv(str(path), index=False, encoding="utf-8")


if __name__ == '__main__':

    FOLDER_NAME = input('Election subfolder: ')
    SOURCE = DATA_FOLDER / FOLDER_NAME
    TARGET = DATA_FOLDER / 'csv' / FOLDER_NAME
    VOTE_COUNTS = TARGET / 'vote_counts'
    VOTE_COUNTS.mkdir(exist_ok=True, parents=True)

    print('\nDo you also want to create a csv with results per candidate?')
    print('This will take up more disk space (~1GB)\n')
    PER_CANDIDATE = input('Per candidate (y/n): ')

    print('\nDo you also want to create a csv with turnout, uncounted, and rejected votes?')
    print('This will take up more disk space (~100MB)\n')
    TURNOUT = input('Turnout (y/n): ')

    start = dt.datetime.now()

    process_files()

    if PER_CANDIDATE in ['yes', 'y']:
        create_candidate_list()

    duration = dt.datetime.now() - start
    print('Duration: {} seconds'.format(round(duration.total_seconds())))