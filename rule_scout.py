from collections.abc import Generator, Iterable
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
import json
import json as jsonmodule
from os import getenv
import re
from typing import Any, Literal
from xml.etree import ElementTree
import httpx
from httpx_retries import Retry, RetryTransport


NOTION_RULE_DATABASE = getenv('NOTION_RULE_DATABASE', '')


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
    title: str
    url: str
    type: Literal['rulemaking', 'nonrulemaking']
    keywords: list[str] = field(default_factory=list)
    rin: str | None = None

    @staticmethod
    def from_api(data: dict) -> 'Docket':
        """Parse a Docket object from a regulations.gov API response."""
        docket_id = data['id']
        rin = data['attributes']['rin']
        if rin and rin.lower() == 'not assigned':
            rin = None

        keywords = [
            term.strip(', ')
            for term in data['attributes']['keywords'] or []
        ]
        # Handle a common case of several terms listed as a single
        # comma-separated string instead of as multiple terms in the list.
        #
        # It's important to check for comma + space and not just comma. We
        # frequently see IUPAC nomenclature for organic molecules here, which
        # uses comma-separated numbers to describe chains of carbons, e.g.
        # "(Z)-1-Chloro-2,3,3,3,-Tetrafluoropropene".
        if len(keywords) == 1 and ', ' in keywords[0]:
            keywords = re.split(r',\s+', keywords[0])

        return Docket(
            id=docket_id,
            title=data['attributes']['title'],
            url=f'https://www.regulations.gov/docket/{docket_id}',
            type=data['attributes']['docketType'].lower(),
            # TODO: the comma substitution here is because Notion can't handle
            # commas in select box items. This should probably happen when
            # formatting Notion input and not here.
            keywords=[re.sub(r',', ";", term) for term in keywords],
            rin=rin
        )


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


class HttpClient(httpx.Client):
    def __init__(self, timeout: float = 10.0, transport: Any = None, **kwargs):
        super().__init__(
            timeout=timeout,
            transport=(transport or RetryTransport(retry=Retry(total=5, backoff_factor=1.0))),
            **kwargs,
        )


class NotionApi(HttpClient):
    BASE_URL = 'https://api.notion.com/v1'

    def __init__(self, api_key):
        if not isinstance(api_key, str):
            raise TypeError('api_key must be a string')

        super().__init__(
            base_url=self.BASE_URL,
            headers={
                'Authorization': f'Bearer {api_key}',
                'Notion-Version': '2025-09-03',
                'Content-Type': 'application/json',
            },
            timeout=15.0,
        )

    def json(
        self,
        method: str,
        url: str,
        *,
        json: Any = None,
        params: Any = None,
        headers: dict | None = None,
    ) -> dict | list:
        response = self.request(method, url, json=json, params=params, headers=headers)
        body = response.json()
        if not response.is_success:
            raise ValueError(f'Error for Notion {method} {url}: {jsonmodule.dumps(body, indent=2)}')

        return body

    def query_db(self, data_source_id: str, filter: dict | None = None, select: list[str] = [], sort: dict[str, str] = {}) -> Generator[dict, None, None]:
        body = {}
        if filter:
            body['filter'] = filter
        if sort:
            body['sorts'] = [
                dict(property=key, direction=value)
                for key, value in sort.items()
            ]
        params = {}
        if select:
            params['filter_properties[]'] = select

        while True:
            data = self.json(
                'POST',
                url=f'/data_sources/{data_source_id}/query',
                params=params,
                json=body,
            )
            assert isinstance(data, dict)

            yield from data['results']

            if data.get('has_more', False):
                body = {**body, 'start_cursor': data.get('next_cursor')}
            else:
                break

    def insert_into_db(self, data_source_id: str, page_data: dict) -> Any:
        response = self.post(
            url='/pages',
            json={
                'parent': {
                    'type': 'data_source_id',
                    'data_source_id': data_source_id,
                },
                'properties': page_data,
            },
        )

        body = response.json()
        if not response.is_success:
            raise ValueError(f'Error inserting into Notion DB: {json.dumps(body, indent=2)}')

        return body

    def get_page(self, page_id: str) -> Any:
        return self.json('GET', f'/pages/{page_id}')

    def get_page_content(self, page_id: str) -> Any:
        content = []
        next_options = {
            'url': f'/blocks/{page_id}/children',
            'params': {}
        }
        while next_options:
            data = self.get(**next_options).raise_for_status().json()
            content.extend(data['results'])
            if data['next_cursor']:
                next_options = {
                    **next_options,
                    'params': {'start_cursor': data['next_cursor']}
                }
            else:
                next_options = None

        return content

    def update_page(self, page_id: str, properties: dict) -> Any:
        response = self.patch(
            url=f'/pages/{page_id}',
            json={'properties': properties}
        )

        body = response.json()
        if not response.is_success:
            raise ValueError(f'Error updating page {page_id}: {json.dumps(body, indent=2)}')

        return body

    def trash_page(self, page_id: str) -> Any:
        response = self.patch(
            url=f'/pages/{page_id}',
            json={'in_trash': True}
        )

        body = response.json()
        if not response.is_success:
            raise ValueError(f'Error trashing page {page_id}: {json.dumps(body, indent=2)}')

        return body

    def append_page_content(self, page_id: str, children: list[dict], after: str|None) -> Any:
        return self.json(
            'PATCH',
            url=f'/blocks/{page_id}/children',
            json={'children': children, 'after': after}
        )

    @staticmethod
    def cell_as_text(cell: dict, type_field: str | None = None) -> str | None:
        cell_type = type_field or cell['type']
        raw_value = cell[cell_type]
        if raw_value:
            return ''.join(part['plain_text'] for part in raw_value)
        else:
            return None

    @staticmethod
    def cell_as_datetime(cell: dict) -> datetime | None:
        if cell['type'] != 'date':
            raise TypeError(f'Cell is not a date (type={cell['type']})')

        notion_value = cell['date']
        if notion_value:
            result = datetime.fromisoformat(notion_value['start'])
            if not result.tzinfo:
                result = result.astimezone(timezone.utc)

            return result
        else:
            return None


class FederalRegisterApi(HttpClient):
    BASE_URL = 'https://www.federalregister.gov/api/v1'

    def __init__(self):
        super().__init__(base_url=self.BASE_URL)

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


class RegulationsGovApi(HttpClient):
    BASE_URL = 'https://api.regulations.gov/v4'

    def __init__(self, api_key):
        if not isinstance(api_key, str):
            raise TypeError('api_key must be a string')

        super().__init__(
            base_url=self.BASE_URL,
            headers={'X-Api-Key': api_key},
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

                    # Not all documents belong to [visible] dockets! Usually
                    # this is because an agency (e.g. FCC) does not use
                    # regulations.gov (often because they have their own
                    # public comment system). It seems the proposed rules get
                    # posted somehow to regulations.gov, but are added to a
                    # special docket that is not visible to public users, and
                    # that was probably automatically created.
                    docket_id = document_info['attributes']['docketId']
                    if docket_id:
                        document.docket = Docket.from_api(regulations_gov.get_docket(docket_id))
                        if document.docket.rin and document.docket.rin not in data.rins:
                            data.rins.append(document.docket.rin)

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
                        # These are now relations and need to be formatted
                        # differently:
                        #   {'type': 'relation', 'relation': [{'id': '<page_id>'}]}
                        # 'Corrections': notion_rich_text(', '.join(data.corrections)),
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
                        },
                        # Combined list of FR topics and Docket keywords.
                        'Tags': {
                            'type': 'multi_select',
                            'multi_select': [
                                {'name': re.sub(r', ', ' and ', topic)}
                                for topic in (*data.fr_topics, *keywords,)
                            ]
                        },
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
