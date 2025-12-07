from collections.abc import Generator
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
import json
from os import getenv
import re
from typing import Any, Iterable
from xml.etree import ElementTree
import httpx


NOTION_RULE_DATABASE = getenv('NOTION_RULE_DATABASE')


@dataclass
class FrAgency:
    id: int
    name: str
    short_name: str | None = None
    url: str | None = None
    description: str | None = None
    fr_slug: str | None = None


@dataclass
class Docket:
    id: str
    url: str
    keywords: list[str] = field(default_factory=list)
    rin: str | None = None


@dataclass
class DocketDocument:
    id: str
    url: str
    comment_start_date: datetime | None = None
    comment_end_date: datetime | None = None
    docket: Docket | None = None


@dataclass
class ProposedRule:
    title: str
    abstract: str
    agencies: list[FrAgency]
    authority: list[str]
    corrections: list
    fr_citation: str
    fr_document_number: str
    fr_html: str
    fr_pdf: str
    fr_publication_date: date
    fr_topics: list[str]
    comment_end_date: date | None
    rins: list[str]
    docket_documents: list[DocketDocument] = field(default_factory=list)


class NotionApi(httpx.Client):
    BASE_URL = 'https://api.notion.com/v1'

    def __init__(self, api_key):
        if not isinstance(api_key, str):
            raise TypeError('api_key must be a string')

        super().__init__(
            base_url=self.BASE_URL,
            headers={
                'Authorization': f'Bearer {api_key}',
                'Notion-Version': '2022-06-28',
                'Content-Type': 'application/json',
            },
            timeout=15.0,
        )

    def query_db(self, block_id: str, filter: dict | None = None) -> dict:
        body = {}
        if filter:
            body['filter'] = filter

        data = self.post(
            url=f'/databases/{block_id}/query',
            json=body
        ).json()

        return data['results']

    def insert_into_db(self, block_id: str, page_data: dict) -> Any:
        response = self.post(
            url='/pages',
            json={
                'parent': {'type': 'database_id', 'database_id': block_id},
                'properties': page_data
            }
        )

        body = response.json()
        if not response.is_success:
            raise ValueError(f'Error inserting into Notion DB: {json.dumps(body, indent=2)}')

        return body

    @staticmethod
    def cell_as_text(cell: dict) -> str | None:
        cell_type = cell['type']
        raw_value = cell[cell_type]
        if raw_value:
            return ''.join(part['plain_text'] for part in raw_value)
        else:
            return None


class FederalRegisterApi(httpx.Client):
    BASE_URL = 'https://www.federalregister.gov/api/v1'

    def __init__(self):
        super().__init__(base_url=self.BASE_URL, timeout=10.0)

    def get_document(self, document_id) -> dict:
        return self.get(url=f'/documents/{document_id}').raise_for_status().json()

    def get_recent_proposed_rules(self, from_date: date | None = None, to_date: date | None = None) -> Generator[dict]:
        params = {
            'order': 'oldest',
            'conditions[type][]': 'PRORULE',
        }
        if from_date:
            params['conditions[publication_date][gte]'] = from_date.isoformat()
        if to_date:
            params['conditions[publication_date][lte]'] = to_date.isoformat()
        next_options = {
            'url': '/documents',
            'params': params
        }
        while next_options:
            page = self.get(**next_options).raise_for_status().json()
            yield from page.get('results') or []

            next_url = page.get('next_page_url')
            next_options = {'url': next_url} if next_url else None

    def get_rule_authority(self, rule_info) -> list[str]:
        xml_url = rule_info['full_text_xml_url']
        if not xml_url:
            return []

        gpo_xml = self.get(xml_url).raise_for_status().text
        root = ElementTree.fromstring(gpo_xml)

        # Find all <AUTH> elements and extract paragraph content (they usually
        # also contain a heading).
        auth_texts = (auth.text
                      for auth in root.findall('.//AUTH/P')
                      if auth.text is not None)
        return [item.strip()
                for text in auth_texts
                for item in text.split(';')]


class RegulationsGovApi(httpx.Client):
    BASE_URL = 'https://api.regulations.gov/v4'

    def __init__(self, api_key):
        if not isinstance(api_key, str):
            raise TypeError('api_key must be a string')

        super().__init__(
            base_url=self.BASE_URL,
            headers={'X-Api-Key': api_key},
            timeout=10.0,
        )

    def get_docket(self, docket_id) -> dict:
        response = self.get(url=f'/dockets/{docket_id}')
        return response.raise_for_status().json()['data']

    def get_document(self, document_id) -> dict:
        response = self.get(url=f'/documents/{document_id}')
        return response.raise_for_status().json()['data']

    def find_documents_by_register_id(self, register_id) -> list[dict]:
        results = self.get(
            url='/documents',
            params={'filter[frDocNum]': register_id}
        ).raise_for_status().json()

        return results['data']


def main() -> None:
    timeframe = timedelta(days=2)
    from_date = date.today() - timeframe

    with NotionApi(getenv('NOTION_API_KEY')) as notion:
        rule_rows = notion.query_db(
            NOTION_RULE_DATABASE,
            {
                'property': 'FR Document Number',
                'rich_text': {
                    'is_not_empty': True
                }
            }
        )

        already_in_notion = set(notion.cell_as_text(row['properties']['FR Document Number'])
                                for row in rule_rows)

    with FederalRegisterApi() as register:
        with RegulationsGovApi(getenv(key='REGULATIONS_GOV_API_KEY')) as regulations_gov:
            for rule in register.get_recent_proposed_rules(from_date=from_date):
                register_id = rule['document_number']
                if register_id in already_in_notion:
                    continue

                rule_info = register.get_document(register_id)
                # TODO: check if correction and update existing record instead
                # of skipping. This will be a URL, so we have to parse, e.g:
                # "https://www.federalregister.gov/api/v1/documents/2024-22385"
                if rule_info['correction_of']:
                    continue

                authority = register.get_rule_authority(rule_info)

                data = ProposedRule(
                    title=rule_info['title'],
                    # Sometimes there is markup in here. Mainly I've seen <inf>
                    # (or <E T="52">, which is the same but in GPO XML) for
                    # subscript. I don't think there's a good way to mark that
                    # up in Notion (maybe as an equation?) for now, so just rip
                    # out the markup.
                    abstract=re.sub(r'</?\w+[^>]*>', '', rule_info['abstract']),
                    agencies=[
                        FrAgency(id=agency['id'], name=agency['name'])
                        for agency in rule_info['agencies']
                        # Some listings seem malformed! So far we've only seen:
                        #   {'raw_name': 'Office of Inspector General'}
                        # Which might be a special case?
                        if 'id' in agency
                    ],
                    authority=authority,
                    corrections=[],
                    fr_citation=rule_info['citation'],
                    fr_document_number=register_id,
                    fr_html=rule_info['html_url'],
                    fr_pdf=rule_info['pdf_url'],
                    fr_publication_date=date.fromisoformat(rule_info['publication_date']),
                    fr_topics=sorted(set(rule_info['topics'])),
                    rins=rule_info['regulation_id_numbers'],
                    # This info is not always present and is less detailed than
                    # the equivalent from regulations.gov, so we'll also look
                    # for it there, too.
                    comment_end_date=(
                        date.fromisoformat(rule_info['comments_close_on'])
                        if rule_info['comments_close_on']
                        else None
                    )
                )

                for summary in regulations_gov.find_documents_by_register_id(register_id):
                    regs_gov_id = summary['id']
                    document_info = regulations_gov.get_document(regs_gov_id)

                    comment_start_iso = document_info['attributes']['commentStartDate']
                    comment_end_iso = document_info['attributes']['commentEndDate']
                    document = DocketDocument(
                        id=regs_gov_id,
                        url=f'https://www.regulations.gov/document/{regs_gov_id}',
                        comment_start_date=comment_start_iso and datetime.fromisoformat(comment_start_iso),
                        comment_end_date=comment_end_iso and datetime.fromisoformat(comment_end_iso),
                    )
                    data.docket_documents.append(document)

                    docket_id = document_info['attributes']['docketId']
                    if docket_id:
                        docket_info = regulations_gov.get_docket(docket_id)
                        rin = docket_info['attributes']['rin']
                        document.docket = Docket(
                            id=docket_id,
                            url=f'https://www.regulations.gov/docket/{docket_info["id"]}',
                            keywords=[
                                # Sometimes the keywords end in commas. Other
                                # times they are chemical names with commas,
                                # which we replace with `'` primes.
                                re.sub(r',', "'", term.strip(', '))
                                for term in docket_info['attributes']['keywords'] or []
                            ],
                            rin=rin
                        )
                        if rin and rin not in data.rins:
                            data.rins.append(rin)

                print('\nRule Data:')
                for k, v in asdict(data).items():
                    if k != 'docket_documents':
                        print(f'  {k.ljust(25, '.')} {v}')
                print('  docket_documents:')
                for document in data.docket_documents:
                    print('    -')
                    for k, v in asdict(document).items():
                        print(f'    {k.ljust(23, '.')} {v}')

                comment_end: datetime | date | None = None
                commentable: list[DocketDocument] = sorted(
                    (d for d in data.docket_documents if d.comment_end_date),
                    key=lambda d: d.comment_end_date
                )
                if len(commentable):
                    comment_end = commentable[-1].comment_end_date
                else:
                    comment_end = data.comment_end_date

                # Notion can't take text segments of more than 2k characters.
                # https://developers.notion.com/reference/request-limits#limits-for-property-values
                # Technically we could split up authority info into more
                # segments, but that is fairly complicated (we still have a
                # limit on the number of segments, too) and probably not worth
                # much. An authority block this long just doesn't make that
                # much sense in a page property, instead of content.
                authority_string = '; '.join(data.authority)
                if len(authority_string) >= 2000:
                    authority_string = authority_string[:1999] + '…'

                # Dedupe when multiple dockets use the same keywords.
                keywords = sorted(set(
                    keyword
                    for document in data.docket_documents
                    if document.docket
                    for keyword in document.docket.keywords
                ))

                with NotionApi(getenv('NOTION_API_KEY')) as notion:
                    notion.insert_into_db(NOTION_RULE_DATABASE, {
                        'Corrections': notion_rich_text(', '.join(data.corrections)),
                        'FR Citation': notion_rich_text(data.fr_citation),
                        'FR Topics': {
                            'type': 'multi_select',
                            'multi_select': [
                                {'name': re.sub(r', ', ' and ', topic)}
                                for topic in data.fr_topics
                            ]
                        },
                        'FR Document Number': notion_rich_text(data.fr_document_number),
                        'FR PDF': {
                            'url': data.fr_pdf
                        },
                        'Docket Documents': {
                            'type': 'rich_text',
                            'rich_text': notion_rich_text_url_list(
                                (d.id, d.url)
                                for d in data.docket_documents
                            )
                        },
                        'Docket Keywords': {
                            'type': 'multi_select',
                            'multi_select': [
                                {'name': keyword}
                                for keyword in keywords
                            ]
                        },
                        'FR Publication Date': {
                            'type': 'date',
                            'date': {
                                'start': data.fr_publication_date.isoformat()
                            } if data.fr_publication_date else None
                        },
                        'Dockets': {
                            'type': 'rich_text',
                            'rich_text': notion_rich_text_url_list(
                                (d.docket.id, d.docket.url)
                                for d in data.docket_documents
                                if d.docket
                            )
                        },
                        'RINs': notion_rich_text(', '.join(data.rins)),
                        'Abstract': notion_rich_text(data.abstract),
                        'Rule Name': notion_rich_text(data.title),
                        'Title': {
                            'type': 'title',
                            'title': [notion_text(data.title)]
                        },
                        'Authority': notion_rich_text(authority_string),
                        'Agencies': {
                            'type': 'multi_select',
                            'multi_select': [
                                {'name': re.sub(r'\s*,\s*', ' - ', agency.name)}
                                for agency in data.agencies
                            ]
                        },
                        'Comment End Date': {
                            'type': 'date',
                            'date': {
                                'start': comment_end.isoformat()
                            } if comment_end else None
                        },
                        'FR Link': {
                            'url': data.fr_html
                        }
                    })

    print('Done!')


def notion_rich_text(text: str | None) -> dict:
    segments = []
    if text:
        segment_length = 2000
        max_segments = 100
        segments = []
        remainder = text
        while remainder and len(segments) < max_segments:
            segments.append(notion_text(remainder[:segment_length]))
            remainder = remainder[segment_length:]

    return {
        'type': 'rich_text',
        'rich_text': segments
    }


def notion_text(text: str, link: str | None = None) -> dict:
    return {
        'type': 'text',
        'text': {
            'content': text,
            'link': {'url': link} if link else None
        }
    }


def notion_rich_text_url_list(items: Iterable[tuple[str, str]]) -> list[dict]:
    result = []
    for index, item in enumerate(items):
        if index > 0:
            result.append(notion_text(', '))

        result.append(notion_text(item[0], item[1]))

    return result



if __name__ == "__main__":
    main()
